package au.org.ala.biocache.persistence

import java.io.{FileWriter, File}
import java.nio.ByteBuffer
import java.{lang, util}
import java.util.{Date, UUID}
import java.util.concurrent._

import au.org.ala.biocache.Config
import au.org.ala.biocache.util.Json
import com.datastax.driver.core._
import com.datastax.driver.core.policies._
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.extras.codecs.MappingCodec
import com.google.common.collect.MapMaker
import com.google.common.util.concurrent._
import com.google.inject.Inject
import com.google.inject.name.Named
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.DateUtils
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import scala.collection.{mutable, JavaConversions}

/**
  * Cassandra 3 based implementation of a persistence manager.
  * This should maintain most of the cassandra 3 logic.
  */
class Cassandra3PersistenceManager  @Inject() (
                  @Named("cassandra.hosts") val host:String = "localhost",
                  @Named("cassandra.port") val port:Int = 9042,
                  @Named("cassandra.pool") val poolName:String = "biocache-store-pool",
                  @Named("cassandra.keyspace") val keyspace:String = "occ"
                                              ) extends PersistenceManager {

  import JavaConversions._

  val logger = LoggerFactory.getLogger("Cassandra3PersistenceManager")

  val cluster = {
    val hosts = host.split(",")
    val builder = Cluster.builder()
    hosts.foreach { builder.addContactPoint(_)}
    builder.withReconnectionPolicy(new ExponentialReconnectionPolicy(10000, 60000))
    builder.withCodecRegistry(CodecRegistry.DEFAULT_INSTANCE.register(new TimestampAsStringCodec))
    builder.build()
  }

  val cassandraKeyWordMap = Map(
    "order" -> "bioorder",
    "class" -> "classs"
  )


  val updateThreadService = null

  val session = cluster.connect(keyspace)

  val map = new MapMaker().weakValues().makeMap[String, PreparedStatement]()

  private def getPreparedStmt(query:String) : PreparedStatement = {

    val lookup = map.get(query.toLowerCase)

    if(lookup != null){
      return lookup
    } else {
      try {
        val preparedStatement = session.prepare(query)
        synchronized {
          map.put(query.toLowerCase, preparedStatement)
        }
        preparedStatement
      } catch {
        case e:Exception => {
          logger.error("problem creating a statement for query: " + query)
          logger.error( e.getMessage, e)
          throw e
        }
      }
    }
  }

  def rowKeyExists(rowKey:String, entityName:String) : Boolean = {
    val stmt = getPreparedStmt(s"SELECT rowkey FROM $entityName where rowkey = ? ALLOW FILTERING")
    val boundStatement = stmt bind rowKey
    val rs = session.execute(boundStatement)
    rs.size != 0
  }

  /**
    * Retrieve the a list of key value pairs for the supplied UUID.
    *
    * @param rowKey
    * @param entityName
    * @return
    */
  def get(rowKey:String, entityName:String) : Option[Map[String, String]] = {
    val stmt = getPreparedStmt(s"SELECT * FROM $entityName where rowkey = ? ALLOW FILTERING")
    val boundStatement = stmt bind rowKey
    val rs = session.execute(boundStatement)
    val rows = rs.iterator
    if (rows.hasNext()) {
      val row = rows.next()
      val map = new util.HashMap[String,String]()
      row.getColumnDefinitions.foreach { defin =>
        val value = row.getString(defin.getName)
        if(value != null){
          map.put(defin.getName, value)
        }
      }
      Some(map.toMap)
    } else {
      None
    }
  }

  /**
    * Retrieves a list of columns and the last time in ms that they were modified.
    *
    * This will support removing columns that were not updated during a reload
    */
  def getColumnsWithTimestamps(uuid:String, entityName:String): Option[Map[String, Long]] = {
    val stmt = getPreparedStmt(s"SELECT * FROM $entityName where rowkey = ?")
    val boundStatement = stmt bind uuid
    val rs = session.execute(boundStatement)
    val rows = rs.iterator
    if (rows.hasNext()) {
      val row = rows.next()
      val map = new util.HashMap[String, Long]()

      //filter for columns with values
      val columnsWitHValues = row.getColumnDefinitions.filter { defin =>
        val value = row.getString(defin.getName)
        defin.getName != "rowkey" && defin.getName != "dataresourceuid" && value != null && value != ""
      }

      //construct the select clause
      val selectClause = columnsWitHValues.map { "writetime(" + _.getName + ")" }.mkString(",")

      //retrieve column timestamps
      val wtStmt = getPreparedStmt(s"SELECT $selectClause FROM $entityName where rowkey = ?")
      val wtBoundStatement = wtStmt bind uuid
      val rs2 = session.execute(wtBoundStatement)
      val writeTimeRow = rs2.iterator.next()

      //create the columnName -> writeTime map.
      columnsWitHValues.foreach { defin =>
        val value = row.getString(defin.getName)
        val writetime = writeTimeRow.getLong("writetime(" + defin.getName + ")")
        if(value != null){
          map.put(defin.getName, writetime)
        }
      }
      Some(map.toMap)
    } else {
      None
    }
  }

  /**
    * Retrieve an array of objects, parsing the JSON stored.
    *
    * We are storing rows keyed against unique id's that we don't wish to expose
    * to users. We wish to expose a static UUID to the uses. This UUID will be
    * indexed against thus is queryable.
    *
    * The performance of the index is slightly worse that lookup by key. This should
    * be alright because the index should only be hit for reads via webapp.
    *
    */
  def getByIndex(indexedValue:String, entityName:String, idxColumn:String) : Option[Map[String,String]] = {
    val rowkey = getRowkeyByIndex(indexedValue, entityName, idxColumn)
    if(!rowkey.isEmpty){
      get(rowkey.get, entityName)
    } else {
      None
    }
  }

  /**
    * Does a lookup against an index table which has the form
    *  table: occ
    *  field to index: uuid
    *  index_table: occ_uuid
    *
    *
    * Retrieves a specific property value using the index as the retrieval method.
    */
  def getByIndex(indexedValue:String, entityName:String, idxColumn:String, propertyName:String) = {
    val rowkey = getRowkeyByIndex(indexedValue, entityName, idxColumn)
    if("rowkey".equals(propertyName)){
      rowkey
    } else {
      if(!rowkey.isEmpty){
        get(rowkey.get, entityName, propertyName)
      } else {
        None
      }
    }
  }

  private def getRowkeyByIndex(indexedValue:String, entityName:String, idxColumn:String) : Option[String] = {

    val indexTable = entityName + "_" + idxColumn
    val stmt = getPreparedStmt(s"SELECT value FROM $indexTable where rowkey = ?")
    val boundStatement = stmt bind indexedValue
    val rs = session.execute(boundStatement)
    val rows = rs.iterator
    if (rows.hasNext()) {
      val row = rows.next()
      val value = row.getString("value")
      Some(value)
    } else {
      None
    }
  }

  /**
    * Retrieve the column value.
    */
  def get(uuid:String, entityName:String, propertyName:String) = {
    val stmt = getPreparedStmt(s"SELECT $propertyName FROM $entityName where rowkey = ?")
    val boundStatement = stmt bind uuid
    val rs = session.execute(boundStatement)
    val rows = rs.iterator
    if (rows.hasNext()) {
      val row = rows.next()
      val value = row.getString(propertyName)
      if(value != null){
        Some(value)
      } else {
        None
      }
    } else {
      None
    }
  }

  /**
    * Only retrieves the supplied fields for the record.
    */
  def getSelected(uuid:String, entityName:String, propertyNames:Seq[String]):Option[Map[String,String]] = {
    val select = QueryBuilder.select(propertyNames:_*).from(entityName)
    val rs = session.execute(select)
    val iter = rs.iterator()
    if(iter.hasNext){
      val row = iter.next()
      val map = new util.HashMap[String,String]()
      var idx = 0
      propertyNames.foreach { name =>
        map.put(name, row.getString(idx))
        idx += 1
      }
      Some(map toMap)
    } else {
      None
    }
  }

  /**
    * Retrieve the column value, and parse from JSON to Array
    */
  def getList[A](uuid:String, entityName:String, propertyName:String, theClass:java.lang.Class[_]): List[A] = {
    val column = get(uuid, entityName, propertyName)
    if (column.isEmpty) {
      List()
    } else {
      val json = new String(column.get)
      Json.toListWithGeneric(json, theClass)
    }
  }

  /**
    * Store the supplied batch of maps of properties as separate columns in cassandra.
    */
  def putBatch(entityName: String, batch: Map[String, Map[String, String]], newRecord:Boolean, removeNullFields: Boolean) = {
    batch.keySet.foreach { rowkey =>
      val map = batch.get(rowkey)
      if(!map.isEmpty) {
        put(rowkey, entityName, map.get, newRecord, removeNullFields)
      }
    }
  }

  /**
   * Store the supplied map of properties as separate columns in cassandra.
   */
  def put(rowkey: String, entityName: String, keyValuePairs: Map[String, String], newRecord:Boolean, removeNullFields: Boolean) = {

    try {

      val keyValuePairsToUse = collection.mutable.Map(keyValuePairs.toSeq: _*)

      if(keyValuePairsToUse.containsKey("rowkey") || keyValuePairsToUse.containsKey("rowKey")){
        keyValuePairsToUse.remove("rowKey")
        keyValuePairsToUse.remove("rowkey") //cassandra 3 is case insensitive, and returns columns lower case
      }


      cassandraKeyWordMap.foreach { case (key:String, value:String) =>
        if(keyValuePairsToUse.containsKey(key)){
          val bioorder = keyValuePairsToUse.getOrElse(key, "")
          keyValuePairsToUse.put(value, bioorder) //cassandra 3 is case insensitive, and returns columns lower case
          keyValuePairsToUse.remove(key)
        }
      }

      //TEMPORARY WORKAROUND  - for clustered key issue with cassandra 3
      if (entityName == "occ" && !keyValuePairsToUse.contains("dataResourceUid")) {
        val dataResourceUid = rowkey.split("\\|")(0)
        keyValuePairsToUse.put("dataResourceUid", dataResourceUid)
      }

      val placeHolders = ",?" * keyValuePairsToUse.size

      val sql =
          s"INSERT INTO $entityName (rowkey," + "\"" +  keyValuePairsToUse.keySet.mkString("\",\"") + "\") VALUES (?" + placeHolders + ")"

      val statement = getPreparedStmt(sql)

      val boundStatement = if (keyValuePairsToUse.size == 1) {
        val values = Array(rowkey, keyValuePairsToUse.values.head)
        statement.bind(values: _*)
      } else {
        val values = Array(rowkey) ++ keyValuePairsToUse.values.toArray[String]
        if(values == null){
          throw new Exception("keyValuePairsToUse are null...")
        }
        if(statement == null){
          throw new Exception("Retrieved statement is null...")
        }
        statement.bind(values: _*)
      }

      boundStatement.setConsistencyLevel(ConsistencyLevel.ONE)
      executeWithRetries(session, boundStatement)

      rowkey
    } catch {
      case e:Exception => {
        logger.error("Problem persisting the following to " + entityName + " - " + e.getMessage)
        keyValuePairs.foreach({case(key, value) => logger.error(s"$key = $value")})
        throw e
      }
    }
  }

//  import com.datastax.driver.mapping.Mapper.Option._;
//
//  def fixType(entity:String, value:String) : Object ={
//    if(entity == "dellog"){
////      timestamp()
//    } else {
//      value
//    }
//  }



  private def executeWithRetries(session:Session, stmt:Statement) : ResultSet = {

    val MAX_QUERY_RETRIES = 10
    var retryCount = 0
    var needToRetry = true
    var resultSet:ResultSet = null
    while(retryCount < MAX_QUERY_RETRIES && needToRetry){
      try {
        resultSet = session.execute(stmt)
        needToRetry = false
        retryCount = 0 //reset
      } catch {
        case e:Exception => {
          logger.error(s"Exception thrown during paging. Retry count $retryCount - " + e.getMessage)
          retryCount = retryCount + 1
          needToRetry = true
          if(retryCount > 5){
            logger.error(s"Backing off for 10 minutes. Retry count $retryCount - " + e.getMessage)
            Thread.sleep(600000)
          } else {
            logger.error(s"Backing off for 5 minutes. Retry count $retryCount - " + e.getMessage)
            Thread.sleep(300000)
          }
        }
      }
    }
    resultSet
  }

  /**
   * Store the supplied property value in the column
   */
  def put(rowKey: String, entityName: String, propertyName: String, propertyValue: String, newRecord:Boolean, removeNullFields: Boolean) = {
    val insertCQL = {
      if(entityName == "occ"){
        s"INSERT INTO $entityName (rowkey, dataresourceuid, $propertyName) VALUES (?,?,?)"
      } else {
        s"INSERT INTO $entityName (rowkey, $propertyName) VALUES (?,?)"
      }
    }
    val statement = getPreparedStmt(insertCQL)
    //FIXME a hack to factor out
    val boundStatement = if(entityName == "occ"){
      val dataResourceUid = rowKey.split("\\|")(0)
      statement.bind(rowKey, dataResourceUid, propertyValue)
    } else {
      statement.bind(rowKey, propertyValue)
    }
    val future = session.execute(boundStatement)
    rowKey
  }

  /**
    * Store arrays in a single column as JSON.
    */
  def putList[A](uuid: String, entityName: String, propertyName: String, newList: Seq[A], theClass:java.lang.Class[_],
                 newRecord: Boolean, overwrite: Boolean, removeNullFields: Boolean) = {

    val recordId = { if(uuid != null) uuid else UUID.randomUUID.toString }

    if (overwrite) {
      //val json = Json.toJSON(newList)
      val json:String = Json.toJSONWithGeneric(newList)
      put(uuid, entityName, propertyName, json, newRecord, removeNullFields)

    } else {

      //retrieve existing values
      val column = get(uuid, entityName, propertyName)
      //if empty, write, if populated resolve
      if (column.isEmpty) {
        //write new values
        val json:String = Json.toJSONWithGeneric(newList)
        put(uuid, entityName, propertyName, json, newRecord, removeNullFields)
      } else {
        //retrieve the existing objects
        val currentList = Json.toListWithGeneric(column.get, theClass)
        var buffer = new ListBuffer[A]

        for (theObject <- currentList) {
          if (!newList.contains(theObject)) {
            //add to buffer
            buffer += theObject
          }
        }

        //PRESERVE UNIQUENESS
        buffer ++= newList

        // check equals
        //val newJson = Json.toJSON(buffer.toList)
        val newJson:String = Json.toJSONWithGeneric(buffer.toList)
        put(uuid, entityName, propertyName, newJson, newRecord, removeNullFields)
      }
    }
    recordId
  }

  def listFieldsForEntity(entityName:String): Seq[String] = {
    val tableMetadata = cluster.getMetadata.getKeyspace("occ").getTable(entityName)
    tableMetadata.getColumns.map(column => column.getName).toList
  }

  def addFieldToEntity(entityName:String, fieldName:String) : Unit = {
    val resultset = session.execute(s"ALTER TABLE $entityName ADD $fieldName varchar")
  }

  /**
    * Pages over all the records with the selected columns.
    *
    * @param columnName The names of the columns that need to be provided for processing by the proc
    */
  def pageOverSelect(entityName:String, proc:((String, Map[String,String]) => Boolean), indexedField:String,
                     indexedFieldValue:String, pageSize:Int, columnName:String*) : Int = {

    val columnsString = "rowkey," + columnName.mkString(",")

    logger.debug("Start: Testing paging over all")

    val columns = Array(columnName:_*).mkString(",")

    //get token value
    val pagingQuery = if(StringUtils.isNotEmpty(indexedFieldValue)) {
      s"SELECT rowkey,$columns FROM $entityName where $indexedField = '$indexedFieldValue' allow filtering"
    } else {
      s"SELECT rowkey,$columns FROM $entityName"
    }

    val rs: ResultSet = session.execute(pagingQuery)
    val rows: util.Iterator[Row] = rs.iterator
    var counter: Int = 0
    val start: Long = System.currentTimeMillis
    while (rows.hasNext) {
      val row = rows.next
      val rowkey = row.getString("rowkey")
      counter += 1
      val map = new util.HashMap[String, String]()
      columnName.foreach { name =>
        val value = row.getString(name)
        if(value != null){
          map.put(name, value)
        }
      }

      val currentTime: Long = System.currentTimeMillis
      logger.debug(row.getString(0) + " - records read: " + counter + ".  Records per sec: " + (counter.toFloat) / ((currentTime - start).toFloat / 1000f) + "  Time taken: " + ((currentTime - start) / 1000))

      proc(rowkey, map.toMap)
    }
    //get token ranges....
    //iterate through each token range....
    logger.info(s"Finished: Testing paging over all. Records paged over: $counter")
    counter
  }

  private def getTokenValue(rowkey:String, entityName:String) : Long = {
    val tokenQuery = s"SELECT token(?) FROM $entityName"
    val rs: ResultSet = session.execute(tokenQuery, rowkey)
    rs.iterator().next().getLong(0)
  }

  /**
    * Pages over the records returns the columns that fit within the startColumn and endColumn range
    */
  def pageOverColumnRange(entityName:String, proc:((String, Map[String,String])=>Boolean), startUuid:String = "", endUuid:String = "", pageSize:Int=1000, startColumn:String="", endColumn:String = "") : Unit =
    throw new RuntimeException("No supported")

  /**
    * Iterate over all occurrences, passing the objects to a function.
    * Function returns a boolean indicating if the paging should continue.
    *
    * @param proc
    * @param startUuid, The uuid of the occurrence at which to start the paging
    */
  def pageOverAll(entityName:String, proc:((String, Map[String, String]) => Boolean),
                  startUuid:String = "", endUuid:String = "", pageSize:Int = 1000) = {

    throw new RuntimeException("Range paging not supported with cassandra 3...")
  }

  /**
    * Iterate over all occurrences using an indexed field, passing the objects to a function.
    * Function returns a boolean indicating if the paging should continue.
    *
    * @param proc
    * @param indexedField the indexed field to page over.
    */
  def pageOverIndexedField(entityName:String,
                           proc:((String, Map[String, String]) => Boolean),
                           indexedField:String = "",
                           indexedFieldValue:String = "",
                           threads:Int,
                           localOnly:Boolean
                          ) : Int = {

    logger.debug(s"Start: Testing paging over indexed field $indexedField : $indexedFieldValue")

    import JavaConversions._

    val tokenRanges = if(localOnly) {
      getTokenRangesForLocalNode
    } else {
      getTokenRanges
    }

    val es = MoreExecutors.getExitingExecutorService(Executors.newFixedThreadPool(threads).asInstanceOf[ThreadPoolExecutor])
    val callables: util.List[Callable[Int]] = new util.ArrayList[Callable[Int]]

    tokenRanges.foreach { tokenRange =>

      val scanTask = new Callable[Int] {
        def call() : Int = {
          val startToken = tokenRange.getStart
          val endToken = tokenRange.getEnd

          val pagingQuery = s"SELECT * FROM $entityName where $indexedField = '$indexedFieldValue' " +
            s"AND token(rowkey) > $startToken AND token(rowkey) <= $endToken " +
            s"allow filtering"

          val stmt = new SimpleStatement(pagingQuery)
          stmt.setFetchSize(100)
          stmt.setIdempotent(true)
          stmt.setReadTimeoutMillis(60000)
          stmt.setConsistencyLevel(ConsistencyLevel.ONE)

          val rs: ResultSet = session.execute(stmt)
          val rows: util.Iterator[Row] = rs.iterator
          var counter = 0
          val start = System.currentTimeMillis
          while (rows.hasNext) {
            val row = rows.next
            val rowkey = row.getString("rowkey")
            counter += 1
            val map = new util.HashMap[String, String]()
            row.getColumnDefinitions.foreach { defin =>
              val value = row.getString(defin.getName)
              if (value != null) {
                map.put(defin.getName, value)
              }
            }

            val currentTime = System.currentTimeMillis
            val currentRowKey = row.getString(0)
            val recordsPerSec = (counter.toFloat) / ((currentTime - start).toFloat / 1000f)
            val timeInSec = (currentTime - start) / 1000

            logger.debug(s"$currentRowKey - records read: $counter.  Records per sec: $recordsPerSec,  Time taken: $timeInSec")

            proc(rowkey, map.toMap)
          }
          0
        }
      }
      callables.add(scanTask)
    }

    logger.info("Starting threads...number of callables " + callables.size())
    val futures: util.List[Future[Int]] = es.invokeAll(callables)
    logger.info("All threads have completed paging")

    var grandTotal: Int = 0
    for (f <- futures) {
      val count:Int = f.get.asInstanceOf[Int]
      grandTotal += count
    }

    //get token ranges....
    //iterate through each token range....
    logger.debug(s"End: Testing paging over indexed field $indexedField : $indexedFieldValue")
    grandTotal
  }

  /***
   * Page over all the records in this local node.
   *
   * @param entityName
   * @param proc
   * @param threads
   */
  private def pageOverLocalNotAsync(entityName:String, proc:((String, Map[String, String], String) => Boolean), threads:Int, columns:Array[String] = Array()) : Int = {

    val MAX_QUERY_RETRIES = 20

    //paging threads, processing threads

    //retrieve token ranges for local node
    val tokenRanges: Array[TokenRange] = getTokenRangesForLocalNode

    val startRange = System.getProperty("startAtTokenRange", "0").toInt

    val completedTokenRanges = System.getProperty("completedTokenRanges", "").split(",")

    val checkpointFile = System.getProperty("tokenRangeCheckPointFile", "/tmp/token-range-checkpoint.txt")

    val tokenRangeCheckPointFile = new File(checkpointFile)

    logger.info(s"Logging to token range checkpoint file $checkpointFile")
    logger.info(s"Starting at token range $startRange")

    val es = MoreExecutors.getExitingExecutorService(Executors.newFixedThreadPool(threads).asInstanceOf[ThreadPoolExecutor])
    val callables: util.List[Callable[Int]] = new util.ArrayList[Callable[Int]]

    //generate a set of callable tasks, each with their own token range...
    for (tokenRangeIdx <- startRange until tokenRanges.length){

      if(!completedTokenRanges.contains(tokenRangeIdx.toString)) {
        val scanTask = new Callable[Int] {

          def hasNextWithRetries(rows: Iterator[Row]): Boolean = {

            val MAX_QUERY_RETRIES = 20
            var retryCount = 0
            var needToRetry = true
            var hasNext = false
            while (retryCount < MAX_QUERY_RETRIES && needToRetry) {
              try {
                hasNext = rows.hasNext
                needToRetry = false
                retryCount = 0 //reset
              } catch {
                case e: Exception => {
                  logger.error(s"Exception thrown during paging. Retry count $retryCount", e)
                  retryCount = retryCount + 1
                  needToRetry = true
                  if (retryCount > 3) {
                    logger.error(s"Backing off for 10 minutes. Retry count $retryCount", e)
                    Thread.sleep(600000)
                  } else {
                    logger.error(s"Backing off for 5 minutes. Retry count $retryCount", e)
                    Thread.sleep(300000)
                  }
                }
              }
            }
            hasNext
          }

          def getNextWithRetries(rows: Iterator[Row]): Row = {

            val MAX_QUERY_RETRIES = 20
            var retryCount = 0
            var needToRetry = true
            var row: Row = null
            while (retryCount < MAX_QUERY_RETRIES && needToRetry) {
              try {
                row = rows.next()
                needToRetry = false
                retryCount = 0 //reset
              } catch {
                case e: Exception => {
                  logger.error(s"Exception thrown during paging. Retry count $retryCount", e)
                  retryCount = retryCount + 1
                  needToRetry = true
                  if (retryCount > 3) {
                    logger.error(s"Backing off for 10 minutes. Retry count $retryCount", e)
                    Thread.sleep(600000)
                  } else {
                    logger.error(s"Backing off for 5 minutes. Retry count $retryCount", e)
                    Thread.sleep(30000)
                  }
                }
              }
            }
            row
          }

          def call(): Int = {

            val columnsString = if (columns.length > 0) {
              columns.mkString(",")
            } else {
              "*"
            }

            logger.debug("Starting token range from " + tokenRanges(tokenRangeIdx).getStart() + " to " + tokenRanges(tokenRangeIdx).getEnd())

            var counter = 0
            val tokenRangeToUse = tokenRanges(tokenRangeIdx)

            val tokenRangesSplits: Seq[TokenRange] = if (Config.cassandraTokenSplit != 1) {
              tokenRangeToUse.splitEvenly(Config.cassandraTokenSplit)
            } else {
              List(tokenRangeToUse)
            }

            tokenRangesSplits.foreach { tokenRange =>
              val startToken = tokenRange.getStart()
              val endToken = tokenRange.getEnd()

              val stmt = new SimpleStatement(s"SELECT $columnsString FROM $entityName where token(rowkey) > $startToken and token(rowkey) <= $endToken")
              stmt.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE) //ensure local reads....
              stmt.setFetchSize(500)
              stmt.setReadTimeoutMillis(120000) //2 minute timeout

              val rs = {
                var retryCount = 0
                var needToRetry = true
                var success: ResultSet = null
                while (retryCount < MAX_QUERY_RETRIES && needToRetry) {
                  try {
                    success = session.execute(stmt)
                    needToRetry = false
                    retryCount = 0 //reset
                  } catch {
                    case e: Exception => {
                      logger.error(s"Exception thrown during paging. Retry count $retryCount", e)
                      retryCount = retryCount + 1
                      needToRetry = true
                      if (retryCount > 3) {
                        logger.error(s"Backing off for 10 minutes. Retry count $retryCount", e)
                        Thread.sleep(600000)
                      } else {
                        logger.error(s"Backing off for 5 minutes. Retry count $retryCount", e)
                        Thread.sleep(300000)
                      }
                    }
                  }
                }
                success
              }

              if (rs != null) {

                val rows = rs.iterator()

                val start = System.currentTimeMillis()

                //need retries
                while (hasNextWithRetries(rows)) {
                  val row = getNextWithRetries(rows)
                  val rowkey = row.getString("rowkey")
                  val map = new util.HashMap[String, String]()
                  row.getColumnDefinitions.foreach { defin =>
                    val value = row.getString(defin.getName)
                    if (value != null) {
                      map.put(defin.getName, value)
                    }
                  }

                  try {
                    //processing - does this want to be on a separate thread ??
                    proc(rowkey, map.toMap, tokenRangeIdx.toString)
                  } catch {
                    case e:Exception => logger.error("Exception throw during paging: " + e.getMessage, e)
                  }

                  counter += 1
                  if (counter % 10000 == 0) {
                    val currentTime = System.currentTimeMillis()
                    val currentRowkey = row.getString(0)
                    val recordsPerSec = (counter.toFloat) / ((currentTime - start).toFloat / 1000f)
                    val totalTimeInSec = ((currentTime - start) / 1000)
                    logger.info(s"[Token range : $tokenRangeIdx] records read: $counter " +
                      s"records per sec: $recordsPerSec  Time taken: $totalTimeInSec seconds, $currentRowkey")
                  }
                }
              }
            }
            logger.info(s"[Token range total count : $tokenRangeIdx] start:" + tokenRangeToUse.getStart.getValue + ", count: " + counter)

            synchronized {
              if (!tokenRangeCheckPointFile.exists()) {
                tokenRangeCheckPointFile.createNewFile()
              }
              val fw = new FileWriter(tokenRangeCheckPointFile, true)
              try {
                fw.write(tokenRangeIdx + "," + counter + "\n")
              }
              finally fw.close()
            }
            counter
          }
        }
        callables.add(scanTask)
      } else {
        logger.info("Skipping token range index :" + tokenRangeIdx)
      }
    }

    logger.info("Starting threads...number of callables " + callables.size())
    val futures: util.List[Future[Int]] = es.invokeAll(callables)
    logger.info("All threads have completed paging")

    var grandTotal: Int = 0
    for (f <- futures) {
      val count:Int = f.get.asInstanceOf[Int]
      grandTotal += count
    }
    grandTotal
  }

  /**
    * Page over data local to the cassandra instance.
    *
    * @param entityName
    * @param proc
    * @param threads
    * @param columns
    * @return
    */
  def pageOverLocal(entityName:String, proc:(String, Map[String, String], String) => Boolean, threads:Int, columns:Array[String] = Array()) : Int = {
//    if(useAsyncPaging){
//      pageOverLocalAsync(entityName, proc, threads, columns)
//    } else {
      pageOverLocalNotAsync(entityName, proc, threads, columns)
//    }
  }


  /**
   * Returns a sorted list of token ranges.
   *
   * @return
   */
  private def getTokenRanges : Array[TokenRange] = {

    val metadata = cluster.getMetadata
    val tokenRanges = unwrapTokenRanges(metadata.getTokenRanges()).toArray(new Array[TokenRange](0))

    logger.info("#######  Using full replication, so splitting token ranges based on node number " + Config.nodeNumber)
    logger.info("#######  Total number of token ranges for cluster " + tokenRanges.length)
    tokenRanges.sortBy(_.getStart)
  }

  /**
    * This returns a set of token ranges associated with this node.
    * If using full replication, then all ranges will associated with a node. To get around this,
    * configuration is used to determine the number of nodes, and then the order of a node.
    * The set of token ranges is then divided into sets for each node.
    *
    * @return an array of token ranges for this node.
    */
  private def getTokenRangesForLocalNode : Array[TokenRange] = {

    val metadata = cluster.getMetadata
    val allHosts = metadata.getAllHosts
    val localHost = {
      var localhost: Host = null
      allHosts.foreach { host =>
        if (host.getAddress().getHostAddress() == Config.localNodeIp) {
          localhost = host
        }
      }
      localhost
    }

    val replicaCount= new util.HashMap[Host, util.List[TokenRange]]
    val tokenRanges = unwrapTokenRanges(metadata.getTokenRanges).toArray(new Array[TokenRange](0))

    val tokenRangesSorted = tokenRanges.sortBy(_.getStart)
    val numberOfHost = metadata.getAllHosts.size
    val rangesPerHost = tokenRanges.length / numberOfHost

    /** ****** Token ranges debug ********/
    tokenRangesSorted.foreach { tokenRange =>

      val hosts = metadata.getReplicas(keyspace, tokenRange)
      var rangeHosts = ""
      val iter = hosts.iterator

      var allocated = false
      while (iter.hasNext && !allocated) {
        val host = iter.next
        var tokenRangesForHost = replicaCount.get(host)
        if (tokenRangesForHost == null) {
          tokenRangesForHost = new util.ArrayList[TokenRange]
        }
        if (tokenRangesForHost.size < rangesPerHost || !iter.hasNext) {
          tokenRangesForHost.add(tokenRange)
          replicaCount.put(host, tokenRangesForHost)

          allocated = true
        }
        rangeHosts += host.getAddress.toString
      }
    }

    replicaCount.keySet.foreach { replica =>
      val allocatedRanges: util.List[TokenRange] = replicaCount.get(replica)
      logger.info(replica.getAddress + " allocated ranges: " + allocatedRanges.size)
      import scala.collection.JavaConversions._
      for (tr <- replicaCount.get(replica)) {
        logger.info(tr.getStart + " to " + tr.getEnd)
      }
    }

    replicaCount.get(localHost).toArray(Array[TokenRange]())
  }

  private def unwrapTokenRanges(wrappedRanges: util.Set[TokenRange]) : util.Set[TokenRange] = {
    import scala.collection.JavaConversions._
    val tokenRanges: util.HashSet[TokenRange] = new util.HashSet[TokenRange]
    for (tokenRange <- wrappedRanges) {
      tokenRanges.addAll(tokenRange.unwrap)
    }
    tokenRanges
  }

  /**
    * Select fields from rows and pass to the supplied function.
    */
  def selectRows(rowkeys:Seq[String], entityName:String, fields:Seq[String], proc:((Map[String, String]) => Unit)) : Unit = {

    val cleanedFields = fields.map { field =>
      if(field == "order"){
        "bioorder"
      } else {
        field
      }
    }

    val fieldsList = cleanedFields.mkString(",")
    val statement = session.prepare(s"SELECT $fieldsList FROM $entityName where rowkey = ?")
    val futures = new ListBuffer[ResultSetFuture]
    rowkeys.foreach { rowkey =>
      val resultSetFuture = session.executeAsync(statement.bind(rowkey))
      futures.add(resultSetFuture)
    }

    futures.foreach { future =>
      val rows = future.getUninterruptibly()
      val row = rows.one()
      val mapBuilder = collection.mutable.Map[String,String]()
      cleanedFields.foreach { field =>
        mapBuilder.put(field, row.getString(field))
      }

      proc(mapBuilder.toMap)
    }
  }

  def shutdown = {
    this.session.close()
    this.cluster.close()
  }

  /**
    * Delete the value for the supplied column
    */
  def deleteColumns(uuid:String, entityName:String, columnName:String*) = {
    throw new RuntimeException("Currently not implemented!!!")
  }

  /**
   * Removes the record for the supplied rowkey from entityName.
   */
  def delete(rowKey:String, entityName:String) = {
    val deleteStmt = getPreparedStmt(s"DELETE FROM $entityName where rowKey = ?")
    val boundStatement = deleteStmt bind rowKey
    session.execute(boundStatement)
  }

  /**
    * The field delimiter to use
    */
  override def fieldDelimiter: Char = '_'

  override def caseInsensitiveFields = true

  /**
   * Creates and populates a secondary index.
   *
   * @param entityName
   * @param indexField
   * @return
   */
  def createSecondaryIndex(entityName:String, indexField:String, threads:Int): Int = {

    try {
      val stmt = new SimpleStatement("CREATE TABLE occ_uuid (rowkey varchar, value varchar, PRIMARY KEY (rowkey));")
      val rs = session.execute(stmt)
    } catch {
      case e:com.datastax.driver.core.exceptions.AlreadyExistsException => println("Index already exists...")
    }

    var counter = 0
    val start = System.currentTimeMillis()
    // create table
    pageOverLocal(entityName, (guid, map, tokenRangeIdx) => {
      //insert into secondary index
      map.get("uuid") match {
        case Some(uuid) => put(uuid, entityName + "_" + indexField, "value", guid, true, false)
        case None => println(s"Record with guid: $guid missing $indexField value")
      }
      counter += 1
      if (counter % 10000 == 0) {
        val currentTime = System.currentTimeMillis()
        logger.info("Records indexed: " + counter + ", records per sec: " +
          ( counter.toFloat) / ( (currentTime - start).toFloat / 1000f) + "  Time taken: " +
          ((currentTime - start) / 1000) + " seconds")
      }
      true
    }, threads, columns = Array("rowkey", indexField))

    counter
  }

  class TimestampAsStringCodec extends MappingCodec(TypeCodec.timestamp(), classOf[String])  {
    def serialize(value:String): Date = { DateUtils.parseDate(value, "yyyy-MM-dd HH:mm:ss") }
    def deserialize(value:Date): String = { org.apache.commons.lang.time.DateFormatUtils.format(new java.util.Date, "yyyy-MM-dd HH:mm:ss") }
  }
}