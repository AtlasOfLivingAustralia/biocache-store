package au.org.ala.biocache.persistence

import java.io.{File, FileWriter}
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.{Date, UUID}

import au.org.ala.biocache.Config
import au.org.ala.biocache.util.Json
import com.datastax.driver.core._
import com.datastax.driver.core.policies._
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.extras.codecs.MappingCodec
import com.google.common.collect.MapMaker
import com.google.common.util.concurrent.{FutureCallback, Futures, MoreExecutors}
import com.google.inject.Inject
import com.google.inject.name.Named
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.DateUtils
import org.slf4j.LoggerFactory

import scala.collection.{JavaConversions, mutable}
import scala.collection.mutable.ListBuffer

/**
  * Cassandra 3 based implementation of a persistence manager.
  * This should maintain most of the cassandra 3 logic.
  */
class Cassandra3PersistenceManager  @Inject() (
                  @Named("cassandra.hosts") val host:String = "localhost",
                  @Named("cassandra.port") val port:Int = 9042,
                  @Named("cassandra.pool") val poolName:String = "biocache-store-pool",
                  @Named("cassandra.keyspace") val keyspace:String = "occ",
                  @Named("cassandra.async.updates.threads") val noOfUpdateThreads:String = "8") extends PersistenceManager {

  import JavaConversions._

  val logger = LoggerFactory.getLogger("Cassandra3PersistenceManager")

  val cluster = {
    val hosts = host.split(",")
    val builder = Cluster.builder()
    hosts.foreach { host: String =>
      val host_port = host.split(":")
      if (host_port.length > 1) {
        //when a specific port is supplied with the host parameter do not use the default port value
        builder.withPort(Integer.parseInt(host_port(1))).addContactPoint(host_port(0))
      } else {
        builder.withPort(port).addContactPoint(host_port(0))
      }
    }
    builder.withReconnectionPolicy(new ExponentialReconnectionPolicy(10000, 60000))
    builder.withCodecRegistry(CodecRegistry.DEFAULT_INSTANCE.register(new TimestampAsStringCodec))
    builder.build()
  }

  val cassandraKeyWordMap = Map(
    "order" -> "bioorder",
    "class" -> "classs"
  )

  val session = cluster.connect(keyspace)

  val map = new MapMaker().weakValues().makeMap[String, PreparedStatement]()

  val updateThreadPool = Executors.newFixedThreadPool(noOfUpdateThreads.toInt).asInstanceOf[ThreadPoolExecutor]

  private def getPreparedStmt(query: String, table: String): PreparedStatement = {

    val lookup =
      if (Config.caseSensitiveCassandra) {
        map.get(query)
      } else {
        map.get(query.toLowerCase)
      }

    if (lookup != null) {
      return lookup
    } else {
      var tryQuery = true
      var result: PreparedStatement = null
      while (tryQuery) {
        tryQuery = false

        try {
          val preparedStatement = session.prepare(query)
          synchronized {
            if (Config.caseSensitiveCassandra) {
              map.put(query, preparedStatement)
            } else {
              map.put(query.toLowerCase, preparedStatement)
            }
          }
          result = preparedStatement
        } catch {
          case missing: com.datastax.driver.core.exceptions.InvalidQueryException if missing.getMessage().startsWith("Undefined column name") => {
            if (Config.createColumnCassandra) {
              //automatically create missing columns
              val columnName = missing.getMessage().replace("Undefined column name", "").replaceAll("\"", "").trim()
              val lcase = columnName.toLowerCase

              //does this column already exist?
              val correctCase = listFieldsForEntity(table).find { fieldName =>
                fieldName.toLowerCase == lcase
              }

              if (correctCase.isEmpty || !Config.caseSensitiveCassandra) {
                logger.error(s"adding missing column '${columnName}' to '$table' for error: ${missing.getMessage()}")
                tryQuery = true
                addFieldToEntity(table, columnName)
              } else {
                logger.error(s"Incorrect column case. requested: '${columnName}', actual: '${correctCase.getOrElse(null)}', query: " + query)
                logger.error(missing.getMessage, missing)
                throw missing
              }
            } else {
              logger.error("problem creating a statement for query: " + query)
              logger.error(missing.getMessage, missing)
              throw missing
            }
          }
          case e: Exception => {
            logger.error("problem creating a statement for query: " + query)
            logger.error(e.getMessage, e)
            throw e
          }
        }
      }
      result
    }
  }

  def rowKeyExists(rowKey: String, entityName: String): Boolean = {
    val stmt = getPreparedStmt(s"SELECT rowkey FROM $entityName where rowkey = ? ALLOW FILTERING", entityName)
    val boundStatement = stmt bind rowKey
    val rs = session.execute(boundStatement)
    rs.size != 0
  }

  /**
    * Retrieve the a list of key value pairs for the supplied rowkey.
    *
    * @param rowKey
    * @param entityName
    * @return
    */
  def get(rowKey: String, entityName: String): Option[Map[String, String]] = {
    val stmt = getPreparedStmt(s"SELECT * FROM $entityName where rowkey = ? ALLOW FILTERING", entityName)
    val boundStatement = stmt bind rowKey
    val rs = session.execute(boundStatement)
    val rows = rs.iterator
    if (rows.hasNext()) {
      val row = rows.next()
      val map = new util.HashMap[String, String]()
      row.getColumnDefinitions.foreach { defin =>
        val value = row.getString(defin.getName)
        if (value != null) {
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
  def getColumnsWithTimestamps(uuid: String, entityName: String): Option[Map[String, Long]] = {
    val stmt = getPreparedStmt(s"SELECT * FROM $entityName where rowkey = ?", entityName)
    val boundStatement = stmt bind uuid
    val rs = session.execute(boundStatement)
    val rows = rs.iterator
    if (rows.hasNext()) {
      val row = rows.next()
      val map = new util.HashMap[String, Long]()

      //filter for columns with values
      val columnsWitHValues = row.getColumnDefinitions.filter { defin =>
        val value = row.getString(defin.getName)
        defin.getName != "rowkey" && defin.getName != "dataResourceUid" && value != null && value != ""
      }

      //construct the select clause
      val selectClause = columnsWitHValues.map {
        "writetime(\"" + _.getName + "\")"
      }.mkString(",")

      //retrieve column timestamps
      val wtStmt = getPreparedStmt(s"SELECT $selectClause FROM $entityName where rowkey = ?", entityName)
      val wtBoundStatement = wtStmt bind uuid
      val rs2 = session.execute(wtBoundStatement)
      val writeTimeRow = rs2.iterator.next()

      //create the columnName -> writeTime map.
      columnsWitHValues.foreach { defin =>
        val value = row.getString(defin.getName)
        val writetime = writeTimeRow.getLong("writetime(\"" + defin.getName + "\")")
        if (value != null) {
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
  def getByIndex(indexedValue: String, entityName: String, idxColumn: String): Option[Map[String, String]] = {
    val rowkey = getRowkeyByIndex(indexedValue, entityName, idxColumn)
    if (!rowkey.isEmpty) {
      get(rowkey.get, entityName)
    } else {
      None
    }
  }

  def getAllByIndex(rowkey:String, entityName:String, idxColumn:String) : Seq[Map[String,String]] = {

    val stmt = getPreparedStmt(s"SELECT * FROM $entityName where $idxColumn = ?", entityName)
    val boundStatement = stmt bind rowkey
    val rs = session.execute(boundStatement)
    val rows = rs.iterator
    if (rows.hasNext()) {
      val list = new ListBuffer[Map[String,String]]

      while(rows.hasNext){
        val row = rows.next()
        val map = mutable.Map[String, String]()
        row.getColumnDefinitions.foreach { defin =>
          val value = row.getString(defin.getName)
          if (value != null) {
            map.put(defin.getName, value)
          }
        }
        list.insert(0, map.toMap)
      }
      list
    } else {
      List()
    }
  }

  /**
    * Does a lookup against an index table which has the form
    * table: occ
    * field to index: uuid
    * index_table: occ_uuid
    *
    *
    * Retrieves a specific property value using the index as the retrieval method.
    */
  def getByIndex(indexedValue: String, entityName: String, idxColumn: String, propertyName: String) = {
    val rowkey = getRowkeyByIndex(indexedValue, entityName, idxColumn)
    if ("rowkey".equals(propertyName)) {
      rowkey
    } else {
      if (!rowkey.isEmpty) {
        get(rowkey.get, entityName, propertyName)
      } else {
        None
      }
    }
  }

  private def getRowkeyByIndex(indexedValue: String, entityName: String, idxColumn: String): Option[String] = {

    val indexTable = entityName + "_" + idxColumn
    val stmt = getPreparedStmt(s"SELECT value FROM $indexTable where rowkey = ?", entityName)
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
  def get(uuid: String, entityName: String, propertyName: String) = {
    val stmt = if (Config.caseSensitiveCassandra) {
      getPreparedStmt("SELECT \"" + propertyName + "\" FROM " + entityName + " where rowkey = ?", entityName)
    } else {
      getPreparedStmt(s"SELECT $propertyName FROM $entityName where rowkey = ?", entityName)
    }
    val boundStatement = stmt bind uuid
    val rs = session.execute(boundStatement)
    val rows = rs.iterator
    if (rows.hasNext()) {
      val row = rows.next()
      val value = row.getString(propertyName)
      Option.apply(value)
    } else {
      None
    }
  }

  /**
    * Only retrieves the supplied fields for the record.
    */
  def getSelected(rowkey: String, entityName: String, propertyNames: Seq[String]): Option[Map[String, String]] = {

    val propertyNameToUse: Seq[String] = if(Config.caseSensitiveCassandra){
      propertyNames.toArray.map { "\"" + _ + "\"" }
    } else {
      propertyNames
    }

    val select = QueryBuilder.select(propertyNameToUse: _*)
      .from(entityName)
      .where(QueryBuilder.eq("rowkey", rowkey))
    val rs = session.execute(select)
    val iter = rs.iterator()
    if (iter.hasNext) {
      val row = iter.next()
      val map = new util.HashMap[String, String]()
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
  def getList[A](uuid: String, entityName: String, propertyName: String, theClass: java.lang.Class[_]): List[A] = {
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
  def putBatch(entityName: String, batch: Map[String, Map[String, String]], newRecord: Boolean, removeNullFields: Boolean) = {
    batch.keySet.foreach { rowkey =>
      val map = batch.get(rowkey)
      if (!map.isEmpty) {
        putAsync(rowkey, entityName, map.get, newRecord, removeNullFields)
      }
    }
  }

  /**
    * Store the supplied map of properties as separate columns in cassandra.
    */
  def put(rowkey: String, entityName: String, keyValuePairs: Map[String, String], newRecord: Boolean, removeNullFields: Boolean) = {

    try {
      val boundStatement: BoundStatement = createPutStatement(rowkey, entityName, keyValuePairs)
      executeWithRetries(session, boundStatement)
      rowkey
    } catch {
      case e: Exception => {
        logger.error("Problem persisting the following to " + entityName + " - " + e.getMessage, e)
        keyValuePairs.foreach({ case (key, value) => logger.error(s"$key = $value") })
        throw e
      }
    }
  }

  /** *
    * Async put implementation.
    * Note this current implementation doesnt use retries.
    *
    * @param rowkey
    * @param entityName
    * @param keyValuePairs
    * @param newRecord
    * @param removeNullFields
    * @return
    */
  def putAsync(rowkey: String, entityName: String, keyValuePairs: Map[String, String], newRecord: Boolean, removeNullFields: Boolean) = {

    try {
      val executor = MoreExecutors.getExitingExecutorService(updateThreadPool)
      val stmt: BoundStatement = createPutStatement(rowkey, entityName, keyValuePairs)
      val future = session.executeAsync(stmt)
      Futures.addCallback(future, new IngestCallback, executor)
      rowkey
    } catch {
      case e: Exception => {
        logger.error("Problem persisting the following to " + entityName + " - " + e.getMessage)
        keyValuePairs.foreach({ case (key, value) => logger.error(s"$key = $value") })
        throw e
      }
    }
  }

  private def createPutStatement(rowkey: String, entityName: String, keyValuePairs: Map[String, String]) : BoundStatement = {
    val keyValuePairsToUse = collection.mutable.Map(keyValuePairs.toSeq: _*)

    if (keyValuePairsToUse.containsKey("rowkey") || keyValuePairsToUse.containsKey("rowKey")) {
      keyValuePairsToUse.remove("rowKey")
      keyValuePairsToUse.remove("rowkey") //cassandra 3 is case insensitive, and returns columns lower case
    }

    if (!Config.caseSensitiveCassandra) {
      cassandraKeyWordMap.foreach { case (key: String, value: String) =>
        if (keyValuePairsToUse.containsKey(key)) {
          val bioorder = keyValuePairsToUse.getOrElse(key, "")
          keyValuePairsToUse.put(value, bioorder) //cassandra 3 is case insensitive, and returns columns lower case
          keyValuePairsToUse.remove(key)
        }
      }
    }

    val placeHolders = ",?" * keyValuePairsToUse.size

    val fieldListForQuery = if (Config.caseSensitiveCassandra){
      keyValuePairsToUse.keySet.toArray.map("\"" + _ + "\"").mkString(",")
    } else {
      keyValuePairsToUse.keySet.mkString(",")
    }


    val sql =
      s"INSERT INTO $entityName (rowkey," + fieldListForQuery + ") VALUES (?" + placeHolders + ")"

    val statement = getPreparedStmt(sql, entityName)

    val boundStatement = if (keyValuePairsToUse.size == 1) {
      val values = Array(rowkey, keyValuePairsToUse.values.head).map { x => new String(x.getBytes("UTF-8")) }
      statement.bind(values: _*)
    } else {
      val values = (Array(rowkey) ++ keyValuePairsToUse.values.toArray[String]).map { x => new String(x.getBytes("UTF-8")) }
      if (values == null) {
        throw new Exception("keyValuePairsToUse are null...")
      }
      if (statement == null) {
        throw new Exception("Retrieved statement is null...")
      }
      statement.bind(values: _*)
    }

    statement.setIdempotent(true) // this will allow retries

    boundStatement
  }

  private def executeWithRetries(session: Session, stmt: Statement): ResultSet = {

    val MAX_QUERY_RETRIES = 10
    var retryCount = 0
    var needToRetry = true
    var resultSet: ResultSet = null
    while (retryCount < MAX_QUERY_RETRIES && needToRetry) {
      try {
        resultSet = session.execute(stmt)
        needToRetry = false
        retryCount = 0 //reset
      } catch {
        case e: Exception => {
          logger.error(s"Exception thrown during paging. Retry count $retryCount - " + e.getMessage)
          retryCount = retryCount + 1
          needToRetry = true
          if (retryCount > 5) {
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
  def put(rowKey: String, entityName: String, propertyName: String, propertyValue: String, newRecord: Boolean, removeNullFields: Boolean) = {
    val insertCQL = {
      if (Config.caseSensitiveCassandra) {
        "INSERT INTO " + entityName + " (rowkey, \"" + propertyName + "\") VALUES (?,?)"
      } else {
        s"INSERT INTO $entityName (rowkey, $propertyName) VALUES (?,?)"
      }
    }
    val statement = getPreparedStmt(insertCQL, entityName)
    val boundStatement = statement.bind(rowKey, propertyValue)
    val future = session.execute(boundStatement)
    rowKey
  }

  /**
    * Store arrays in a single column as JSON.
    */
  def putList[A](rowkey: String, entityName: String, propertyName: String, newList: Seq[A], theClass: java.lang.Class[_],
                 newRecord: Boolean, overwrite: Boolean, removeNullFields: Boolean) = {

    val recordId = {
      if (rowkey != null) rowkey else UUID.randomUUID.toString
    }

    if (overwrite) {
      //val json = Json.toJSON(newList)
      val json: String = Json.toJSONWithGeneric(newList)
      put(rowkey, entityName, propertyName, json, newRecord, removeNullFields)

    } else {

      //retrieve existing values
      val column = get(rowkey, entityName, propertyName)
      //if empty, write, if populated resolve
      if (column.isEmpty) {
        //write new values
        val json: String = Json.toJSONWithGeneric(newList)
        put(rowkey, entityName, propertyName, json, newRecord, removeNullFields)
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
        val newJson: String = Json.toJSONWithGeneric(buffer.toList)
        put(rowkey, entityName, propertyName, newJson, newRecord, removeNullFields)
      }
    }
    recordId
  }

  def listFieldsForEntity(entityName: String): Seq[String] = {
    val tableMetadata = cluster.getMetadata.getKeyspace("occ").getTable(entityName)
    tableMetadata.getColumns.map(column => column.getName).toList
  }

  def addFieldToEntity(entityName: String, fieldName: String): Unit =  if (Config.caseSensitiveCassandra) {
    session.execute("ALTER TABLE " + entityName + " ADD \"" + fieldName + "\" varchar")
  } else {
    session.execute(s"ALTER TABLE $entityName ADD $fieldName varchar")
  }

  /**
    * Pages over all the records with the selected columns.
    *
    * @param columnName The names of the columns that need to be provided for processing by the proc
    */
  def pageOverSelect(entityName: String, proc: ((String, Map[String, String]) => Boolean), pageSize: Int, threads: Int, columnName: String*): Int = {

    val columnsString = if(Config.caseSensitiveCassandra){
      "rowkey," + columnName.toArray.map {"\"" + _ + "\""}.mkString(",")
    } else {
      "rowkey," + columnName.mkString(",")
    }

    logger.debug("Start: paging over with select - " + columnsString)

    val columns = Array(columnName: _*).mkString(",")
    val tokenRanges = getTokenRanges
    val es = MoreExecutors.getExitingExecutorService(Executors.newFixedThreadPool(threads).asInstanceOf[ThreadPoolExecutor])
    val callables: util.List[Callable[Int]] = new util.ArrayList[Callable[Int]]

    tokenRanges.foreach { tokenRange =>

      val scanTask = new Callable[Int] {
        def call(): Int = {
          val startToken = tokenRange.getStart
          val endToken = tokenRange.getEnd

          val pagingQuery = s"SELECT * FROM $entityName where " +
            s"token(rowkey) > $startToken AND token(rowkey) <= $endToken " +
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
      val count: Int = f.get.asInstanceOf[Int]
      grandTotal += count
    }

    //get token ranges....
    //iterate through each token range....
    grandTotal
  }

  def pageOverSelectArray(entityName: String, proc: ((String, GettableData, ColumnDefinitions) => Boolean),
                          indexedField: String, indexedFieldValue: String, pageSize: Int, threads: Int,
                          localOnly: Boolean, columnName: String*): Int = {
    pageOverLocalNotAsync(entityName, null, threads, columnName.toArray, null, indexedField, indexedFieldValue,
      localOnly, proc)
  }

  private def getTokenValue(rowkey: String, entityName: String): Long = {
    val tokenQuery = s"SELECT token(?) FROM $entityName"
    val rs: ResultSet = session.execute(tokenQuery, rowkey)
    rs.iterator().next().getLong(0)
  }

  /**
    * Pages over the records returns the columns that fit within the startColumn and endColumn range
    */
  def pageOverColumnRange(entityName: String, proc: ((String, Map[String, String]) => Boolean), startUuid: String = "", endUuid: String = "", pageSize: Int = 1000, startColumn: String = "", endColumn: String = ""): Unit =
    throw new RuntimeException("Not supported")

  /**
    * Iterate over all occurrences, passing the objects to a function.
    * Function returns a boolean indicating if the paging should continue.
    *
    * @param proc
    * @param startUuid , The uuid of the occurrence at which to start the paging
    */
  def pageOverAll(entityName: String, proc: ((String, Map[String, String]) => Boolean),
                  startUuid: String = "", endUuid: String = "", pageSize: Int = 1000) = {

    if (StringUtils.isNotEmpty(startUuid) || StringUtils.isNotEmpty(endUuid)) {
      throw new RuntimeException("Range paging not supported with cassandra 3...")
    } else {
      pageOverSelect(entityName, proc, pageSize, 1)
    }
  }

  /**
    * Iterate over all occurrences using an indexed field, passing the objects to a function.
    * Function returns a boolean indicating if the paging should continue.
    *
    * @param proc
    * @param indexedField the indexed field to page over.
    */
  def pageOverIndexedField(entityName: String,
                           proc: ((String, Map[String, String]) => Boolean),
                           indexedField: String = "",
                           indexedFieldValue: String = "",
                           threads: Int,
                           localOnly: Boolean
                          ): Int = {

    val count = pageOverLocalNotAsync(entityName, (guid, map, tokenRange) => {
      proc(guid, map)
    }, indexedField = indexedField, indexedFieldValue = indexedFieldValue, threads = threads, localOnly = localOnly)

    count
  }

  /** *
    * Page over all the records in this local node.
    *
    * @param entityName
    * @param proc
    * @param threads
    */
  def pageOverLocalNotAsync(entityName: String, proc: ((String, Map[String, String], String) => Boolean),
                            threads: Int, columns: Array[String] = Array(), rowKeyFile: File = null,
                            indexedField: String = "", indexedFieldValue: String = "",
                            localOnly: Boolean = true,
                            procArray: ((String, GettableData, ColumnDefinitions) => Boolean) = null): Int = {

    val MAX_QUERY_RETRIES = 20

    //paging threads, processing threads

    //retrieve token ranges for local node
    val tokenRanges: Array[TokenRange] =
      if (localOnly) {
        getTokenRangesForLocalNode
      } else {
        getTokenRanges
      }

    val startRange = System.getProperty("startAtTokenRange", "0").toInt

    val completedTokenRanges = System.getProperty("completedTokenRanges", "").split(",")

    val checkpointFile = System.getProperty("tokenRangeCheckPointFile", "/tmp/token-range-checkpoint.txt")

    val tokenRangeCheckPointFile = new File(checkpointFile)

    logger.info(s"Logging to token range checkpoint file $checkpointFile")
    logger.info(s"Starting at token range $startRange")

    val es = MoreExecutors.getExitingExecutorService(Executors.newFixedThreadPool(threads).asInstanceOf[ThreadPoolExecutor])
    val callables: util.List[Callable[Int]] = new util.ArrayList[Callable[Int]]

    val continue = new AtomicBoolean(true)

    //generate a set of callable tasks, each with their own token range...
    for (tokenRangeIdx <- startRange until tokenRanges.length) {

      if (!completedTokenRanges.contains(tokenRangeIdx.toString)) {
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

            if (!continue.get()) {
              return 0
            }

            val columnsString = if (columns.nonEmpty) {
              if (columns.contains("rowkey")) mkString(columns)
              else mkString(columns) + ",rowkey"
            } else {
              "*"
            }

            val filterQuery =
              if (StringUtils.isNotEmpty(indexedFieldValue)) {
                if (Config.caseSensitiveCassandra) {
                  " and \"" + indexedField + "\" = '" + indexedFieldValue + "' allow filtering"
                } else {
                  s" and $indexedField = '$indexedFieldValue' allow filtering"
                }
              } else {
                ""
              }

            logger.debug("Starting token range from " + tokenRanges(tokenRangeIdx).getStart() + " to " + tokenRanges(tokenRangeIdx).getEnd())

            var counter = 0
            val tokenRangeToUse = tokenRanges(tokenRangeIdx)

            val tokenRangesSplits: Seq[TokenRange] = if (Config.cassandraTokenSplit != 1) {
              tokenRangeToUse.splitEvenly(Config.cassandraTokenSplit)
            } else {
              List(tokenRangeToUse)
            }

            tokenRangesSplits.iterator.takeWhile(_ => continue.get).foreach { tokenRange =>
              val startToken = tokenRange.getStart()
              val endToken = tokenRange.getEnd()

              val stmt = new SimpleStatement(s"SELECT $columnsString FROM $entityName where token(rowkey) > $startToken and token(rowkey) <= $endToken " + filterQuery)
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
                while (hasNextWithRetries(rows) && continue.get) {
                  val row = getNextWithRetries(rows)
                  val rowkey = row.getString("rowkey")
                  if (procArray != null) {
                    //process response as an array to avoid conversion to Map
                    try {
                      if (!procArray(rowkey, row, row.getColumnDefinitions)) {
                        continue.set(false)
                      }
                    } catch {
                      case e: Exception => logger.error("Exception throw during paging: " + e.getMessage, e)
                    }
                  } else if (proc != null) {
                    val map = new util.HashMap[String, String]()
                    row.getColumnDefinitions.foreach { defin =>
                      val value = row.getString(defin.getName)
                      if (value != null) {
                        map.put(defin.getName, value)
                      }
                    }

                    try {
                      //processing - does this want to be on a separate thread ??
                      if (!proc(rowkey, map.toMap, tokenRangeIdx.toString)) {
                        continue.set(false)
                      }
                    } catch {
                      case e: Exception => logger.error("Exception throw during paging: " + e.getMessage, e)
                    }
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
              try {
                if (!tokenRangeCheckPointFile.exists()) {
                  tokenRangeCheckPointFile.createNewFile()
                }
                val fw = new FileWriter(tokenRangeCheckPointFile, true)
                try {
                  fw.write(tokenRangeIdx + "," + counter + "\n")
                }
                finally fw.close()
              } catch {
                case e: Exception => {
                  logger.error("failed to create/write to tokenRangeCheckPointFile: " + tokenRangeCheckPointFile.getPath, e)
                }
              }
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
      try {
        val count: Int = f.get
        grandTotal += count
      } catch {
        case e: Exception => println("Index already exists...")
      }
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
  def pageOverLocal(entityName: String, proc: (String, Map[String, String], String) => Boolean, threads: Int, columns: Array[String] = Array()): Int = {
    pageOverLocalNotAsync(entityName, proc, threads, columns)
  }

  /**
    * Returns a sorted list of token ranges.
    *
    * @return
    */
  private def getTokenRanges: Array[TokenRange] = {

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
  private def getTokenRangesForLocalNode: Array[TokenRange] = {

    val metadata = cluster.getMetadata
    val allHosts = metadata.getAllHosts
    val localHosts: ListBuffer[Host] = {
      var localhosts = new ListBuffer[Host]
      allHosts.foreach { host =>
        if (Config.localNodeIp.contains(host.getAddress().getHostAddress())) {
          localhosts.add(host)
        }
      }
      localhosts
    }

    val replicaCount = new util.HashMap[Host, util.List[TokenRange]]
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

    var result : ListBuffer[TokenRange] = new ListBuffer[TokenRange]
    localHosts.foreach { host =>
      result.addAll(replicaCount.get(host))
    }

    result.toList.toArray
  }

  private def unwrapTokenRanges(wrappedRanges: util.Set[TokenRange]): util.Set[TokenRange] = {
    import scala.collection.JavaConversions._
    val tokenRanges: util.HashSet[TokenRange] = new util.HashSet[TokenRange]
    for (tokenRange <- wrappedRanges) {
      tokenRanges.addAll(tokenRange.unwrap)
    }
    tokenRanges
  }

  def mkString(fields: Seq[String]): String = {
    if (Config.caseSensitiveCassandra) {
      val str = fields.mkString("\",\"")
      if (str.length > 0 && str != "*") {
        "\"" + str + "\""
      } else {
        str
      }
    } else {
      fields.mkString(",")
    }
  }

  /**
    * Select fields from rows and pass to the supplied function.
    */
  def selectRows(rowkeys: Seq[String], entityName: String, fields: Seq[String], proc: ((Map[String, String]) => Unit)): Unit = {

    val fieldsList = if (Config.caseSensitiveCassandra) {
      mkString(fields)
    } else {
      val cleanedFields = fields.map { field =>
        if (field == "order") {
          "bioorder"
        } else {
          field
        }
      }
      cleanedFields.mkString(",")
    }
    val statement = session.prepare(s"SELECT $fieldsList FROM $entityName where rowkey = ?")

    // set futures max size relative to threads
    val threads = 8
    val futures = new LinkedBlockingQueue[ResultSetFuture](threads * 2)
    val finished = new AtomicBoolean(false)

    //consumer
    val consumer: Thread = new Thread() {
      override def run(): Unit = {
        while (!finished.get() || futures.size() > 0) {
          val future = futures.poll(100, TimeUnit.MILLISECONDS)
          if (future != null) {
            val rows = future.getUninterruptibly()
            val row = rows.one()
            val mapBuilder = collection.mutable.Map[String, String]()
            if (Config.caseSensitiveCassandra) {
              fields.foreach { field =>
                mapBuilder.put(field, row.getString(field))
              }
            } else {
              fields.foreach { field =>
                mapBuilder.put(field, row.getString(field.toLowerCase))
              }
            }

            proc(mapBuilder.toMap)
          }
        }
      }
    }

    //producer
    val producer: Thread = new Thread() {
      override def run(): Unit = {
        rowkeys.foreach { rowkey =>
          val resultSetFuture = session.executeAsync(statement.bind(rowkey))
          futures.put(resultSetFuture)
        }
      }
    }

    producer.start()
    consumer.start()

    producer.join()
    consumer.join()
  }

  def shutdown = {
    this.session.close()
    this.cluster.close()
  }

  /**
    * Delete the value for the supplied column
    */
  def deleteColumns(uuid: String, entityName: String, columnName: String*) = {
    throw new RuntimeException("Currently not implemented!!!")
  }

  /**
    * Removes the record for the supplied rowkey from entityName.
    */
  def delete(rowKey: String, entityName: String) = {
    val deleteStmt = getPreparedStmt(s"DELETE FROM $entityName where rowkey = ?", entityName)
    val boundStatement = deleteStmt bind rowKey
    val resultSet = session.execute(boundStatement)
    resultSet
  }

  /**
    * Removes the record for the supplied properties from entityName.
    */
  def delete(properties: Map[String, String], entityName: String) = {

    val query = "DELETE FROM " + entityName + " where " +
      properties.keySet.map { "\"" + _ + "\" = ?"}.mkString(" AND ")

    try {
      val deleteStmt = getPreparedStmt(query, entityName)

      val boundStatement =  deleteStmt.bind(properties.values.toArray:_*)

      val resultSet = session.execute(boundStatement)
      resultSet
    } catch {
      case e:Exception => e.printStackTrace()
    }
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
  def createSecondaryIndex(entityName: String, indexField: String, threads: Int): Int = {

    try {
      val stmt = new SimpleStatement("CREATE TABLE occ_uuid (rowkey varchar, value varchar, PRIMARY KEY (rowkey));")
      val rs = session.execute(stmt)
    } catch {
      case e: com.datastax.driver.core.exceptions.AlreadyExistsException => println("Index already exists...")
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
          (counter.toFloat) / ((currentTime - start).toFloat / 1000f) + "  Time taken: " +
          ((currentTime - start) / 1000) + " seconds")
      }
      true
    }, threads, columns = Array("rowkey", indexField))

    counter
  }
}


class TimestampAsStringCodec extends MappingCodec(TypeCodec.timestamp(), classOf[String]) {
  def serialize(value: String): Date = {
    DateUtils.parseDate(value, "yyyy-MM-dd HH:mm:ss")
  }

  def deserialize(value: Date): String = {
    org.apache.commons.lang.time.DateFormatUtils.format(new java.util.Date, "yyyy-MM-dd HH:mm:ss")
  }
}

//callback class
class IngestCallback extends FutureCallback[ResultSet] {
  override def onSuccess(result: ResultSet): Unit = { /*happy days */ }
  override def onFailure(t: Throwable): Unit = {
    throw new RuntimeException(t)
  }
}
