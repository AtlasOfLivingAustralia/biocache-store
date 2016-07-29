package au.org.ala.biocache.persistence

import java.util
import java.util.UUID
import java.util.concurrent._

import au.org.ala.biocache.util.Json
import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.google.common.collect.MapMaker
import com.google.common.util.concurrent.MoreExecutors
import com.google.inject.Inject
import com.google.inject.name.Named
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import scala.collection.{JavaConversions}

/**
  * Cassandra 3 based implementation of a persistence manager.
  * This should maintain most of the cassandra 3 logic.
  */
class Cassandra3PersistenceManager  @Inject() (
                  @Named("cassandra.hosts") val host:String = "localhost",
                  @Named("cassandra.port") val port:Int = 9160,
                  @Named("cassandra.pool") val poolName:String = "biocache-store-pool",
                  @Named("cassandra.keyspace") val keyspace:String = "occ") extends PersistenceManager {

  import JavaConversions._

  val logger = LoggerFactory.getLogger("Cassandra3PersistenceManager")

  val cluster = {
    val hosts = host.split(",")
    val builder = Cluster.builder()
    hosts.foreach { builder.addContactPoint(_)}
    builder.build()
  }

  val session = cluster.connect(keyspace)

  val policy = new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder.build)

  val map = new MapMaker().weakValues().makeMap[String, PreparedStatement]()

  private def getPreparedStmt(query:String) : PreparedStatement = {
    if(map.containsKey(query)){
      return map.get(query)
    } else {
      try {
        val preparedStatement = session.prepare(query)
        map.put(query, preparedStatement)
        return preparedStatement
      } catch {
        case e:Exception => {
          logger.error("problem creating a statement for query: " + query)
          logger.error( e.getMessage, e)
          throw e
        }
      }
    }
  }

  /**
    * Retrieve the a list of key value pairs for the supplied UUID.
    *
    * @param uuid
    * @param entityName
    * @return
    */
  def get(uuid:String, entityName:String) : Option[Map[String, String]] = {
    val stmt = getPreparedStmt(s"SELECT * FROM $entityName where rowkey = ? ALLOW FILTERING")
    val boundStatement = stmt bind uuid
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
      row.getColumnDefinitions.foreach { defin =>
        val value = row.getString(defin.getName)
        if(value != null){
          //FIXME - not sure the column timestamp is supported in cassandra 3
          map.put(defin.getName, System.currentTimeMillis())
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
  def getByIndex(uuid:String, entityName:String, idxColumn:String) : Option[Map[String,String]] = {
    val stmt = getPreparedStmt(s"SELECT * FROM $entityName where $idxColumn = ?")
    val boundStatement = stmt bind uuid
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
    * Retrieves a specific property value using the index as the retrieval method.
    */
  def getByIndex(uuid:String, entityName:String, idxColumn:String, propertyName:String) = {
    val stmt = getPreparedStmt(s"SELECT $propertyName FROM $entityName where $idxColumn = ?")
    val boundStatement = stmt bind uuid
    val rs = session.execute(boundStatement)
    val rows = rs.iterator
    if (rows.hasNext()) {
      val row = rows.next()
      val value = row.getString(propertyName)
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
  def putBatch(entityName: String, batch: Map[String, Map[String, String]], removeNullFields: Boolean) = {
    batch.keySet.foreach { rowkey =>
      val map = batch.get(rowkey)
      if(!map.isEmpty) {
        put(rowkey, entityName, map.get, removeNullFields)
      }
    }
  }

  /**
    * Store the supplied map of properties as separate columns in cassandra.
    */
  def put(uuid: String, entityName: String, keyValuePairs: Map[String, String], removeNullFields: Boolean) = {

    try {

      val keyValuePairsToUse = if(keyValuePairs.containsKey("rowkey") || keyValuePairs.containsKey("rowKey")){
        keyValuePairs.-("rowKey").-("rowkey")
      } else {
        keyValuePairs
      }

      val placeHolders = ",?" * keyValuePairsToUse.size
      val statement = getPreparedStmt(s"INSERT INTO $entityName (rowkey," + keyValuePairsToUse.keySet.mkString(",") + ") VALUES (?" + placeHolders + ")")
      val boundStatement = if (keyValuePairsToUse.size == 1) {
        statement.bind(Array(uuid, keyValuePairsToUse.values.head))
      } else {
        val values = Array(uuid) ++ keyValuePairsToUse.values.toArray[String]
        statement.bind(values: _*)
      }
      val future = session.execute(boundStatement)
      uuid
    } catch {
      case e:Exception => {
        logger.error("Problem persisting the following to " + entityName + " - " + e.getMessage)
        keyValuePairs.foreach({case(key, value) => logger.error(s"$key = $value")})
        throw e
      }
    }
  }

  /**
    * Store the supplied property value in the column
    */
  def put(uuid: String, entityName: String, propertyName: String, propertyValue: String, removeNullFields: Boolean) = {
    val insertCQL = s"INSERT INTO $entityName (rowkey, $propertyName) VALUES (?,?)"
    val statement = getPreparedStmt(insertCQL)
    val boundStatement = statement.bind(uuid, propertyValue)
    val future = session.execute(boundStatement)
    uuid
  }

  /**
    * Store arrays in a single column as JSON.
    */
  def putList[A](uuid: String, entityName: String, propertyName: String, newList: Seq[A], theClass:java.lang.Class[_], overwrite: Boolean, removeNullFields: Boolean) = {

    val recordId = { if(uuid != null) uuid else UUID.randomUUID.toString }

    if (overwrite) {
      //val json = Json.toJSON(newList)
      val json:String = Json.toJSONWithGeneric(newList)
      put(uuid, entityName, propertyName, json, removeNullFields)

    } else {

      //retrieve existing values
      val column = get(uuid, entityName, propertyName)
      //if empty, write, if populated resolve
      if (column.isEmpty) {
        //write new values
        val json:String = Json.toJSONWithGeneric(newList)
        put(uuid, entityName, propertyName, json, removeNullFields)
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
        put(uuid, entityName, propertyName, newJson, removeNullFields)
      }
    }
    recordId
  }

  /**
    * Pages over all the records with the selected columns.
    *
    * @param columnName The names of the columns that need to be provided for processing by the proc
    */
  def pageOverSelect(entityName:String, proc:((String, Map[String,String]) => Boolean), startUuid:String, endUuid:String, pageSize:Int, columnName:String*){
    throw new RuntimeException("No supported")
  }

  /**
    * Pages over the records returns the columns that fit within the startColumn and endColumn range
    */
  def pageOverColumnRange(entityName:String, proc:((String, Map[String,String])=>Boolean), startUuid:String="", endUuid:String="", pageSize:Int=1000, startColumn:String="", endColumn:String="") : Unit =
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

    logger.debug("Start: Testing paging over all")

    //page through the data
    val stmt: Statement = new SimpleStatement(s"SELECT * FROM $entityName")
    stmt.setFetchSize(1000)
    val rs: ResultSet = session.execute(stmt)
    val rows: util.Iterator[Row] = rs.iterator
    var counter: Int = 0
    val start: Long = System.currentTimeMillis
    while (rows.hasNext) {
      val row = rows.next
      val rowkey = row.getString("rowkey")
      counter += 1
      val map = new util.HashMap[String, String]()
      row.getColumnDefinitions.foreach { defin =>
        val value = row.getString(defin.getName)
        if(value != null){
          map.put(defin.getName, value)
        }
      }

      val currentTime: Long = System.currentTimeMillis
      logger.debug(row.getString(0) + " - records read: " + counter + ".  Records per sec: " + (counter.toFloat) / ((currentTime - start).toFloat / 1000f) + "  Time taken: " + ((currentTime - start) / 1000))

      proc(rowkey, map.toMap)
    }
    //get token ranges....
    //iterate through each token range....
    logger.debug("End: Testing paging over all")
  }

  /***
   *
    * @param entityName
   * @param proc
   * @param threads
   */
  def pageOverLocal(entityName:String, proc:((String, Map[String, String]) => Boolean), threads:Int, localNodeIP:String) : Int = {

    val metadata = cluster.getMetadata
    val allHosts = metadata.getAllHosts
    val localHost = {
      var localhost:Host = null
      allHosts.foreach { host =>
        if (host.getAddress().getHostAddress() == localNodeIP) {
          localhost = host
        }
      }
      localhost
    }

    logger.info("####### Retrieving ranges for node:" + localHost.getAddress.getHostAddress)

    val tokenRanges = unwrapTokenRanges(metadata.getTokenRanges(keyspace, localHost)).toArray(new Array[TokenRange](0))
    for (tokenRange <- tokenRanges) {
      logger.info("#######  Token ranges - start:" + tokenRange.getStart + " end:" + tokenRange.getEnd)
    }

    val es = MoreExecutors.getExitingExecutorService(Executors.newFixedThreadPool(threads).asInstanceOf[ThreadPoolExecutor])
    val callables: util.List[Callable[Int]] = new util.ArrayList[Callable[Int]]

    for (tokenRange <- tokenRanges) {

      val scanTask = new Callable[Int]{
        def call() : Int = {
            val stmt = new SimpleStatement("SELECT * FROM occ where token(rowkey) > " + tokenRange.getStart() + " and token(rowkey) < " + tokenRange.getEnd())
            stmt.setFetchSize(1000)
            val rs = session.execute(stmt)
            val rows = rs.iterator()
            var counter = 0
            val start = System.currentTimeMillis()
            while (rows.hasNext()) {
              val row = rows.next()
              val rowkey = row.getString("rowkey")
              counter += 1
              val map = new util.HashMap[String, String]()
              row.getColumnDefinitions.foreach { defin =>
                val value = row.getString(defin.getName)
                if(value != null){
                  map.put(defin.getName, value)
                }
              }

              proc(rowkey, map.toMap)

              counter +=1
              if (counter % 10000 == 0) {
                val currentTime = System.currentTimeMillis()
                logger.info("[" + tokenRange.getStart() + "] " + row.getString(0) + " records read: " + counter + ", records per sec: " +
                  ( counter.toFloat) / ( (currentTime - start).toFloat / 1000f) + "  Time taken: " +
                  ((currentTime - start) / 1000))
              }
            }
            return counter
          }
        }
      callables.add(scanTask)
    }

    val futures: util.List[Future[Int]] = es.invokeAll(callables)
    var grandTotal: Int = 0
    for (f <- futures) {
      val count:Int = f.get.asInstanceOf[Int]
      grandTotal += count
    }
    grandTotal
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
  def selectRows(rowkeys:Seq[String], entityName:String, fields:Seq[String], proc:((Map[String,String])=>Unit)) : Unit =
    throw new RuntimeException("No supported")


  def shutdown = {
    this.session.close()
    this.cluster.close()
  }

  /**
    * Delete the value for the supplied column
    */
  def deleteColumns(uuid:String, entityName:String, columnName:String*) = throw new RuntimeException("Not supported")

  /**
    * Removes the record for the supplied uuid from entityName.
    */
  def delete(uuid:String, entityName:String) = throw new RuntimeException("Not supported")

  /**
    * The field delimiter to use
    */
  override def fieldDelimiter: Char = '_'
}
