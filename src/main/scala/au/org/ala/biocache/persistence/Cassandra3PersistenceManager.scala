package au.org.ala.biocache.persistence

import java.util
import java.util.UUID

import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.QueryBuilder
import scala.collection.mutable.ListBuffer
import scala.collection.{JavaConversions}

/**
  * Cassandra 3 based implementation of a persistence manager.
  * This should maintain most of the cassandra 3 logic.
  */
class Cassandra3PersistenceManager (
                                     val host:String = "localhost",
                                     val port:Int = 9160,
                                     val keyspace:String = "occ") extends PersistenceManager {

  import JavaConversions._

  val cluster = {
    val hosts = host.split(",")
    val builder = Cluster.builder()
    hosts.foreach { builder.addContactPoint(_)}
    builder.build()
  }

  val session = cluster.connect(keyspace)

  /**
    * Retrieve the a list of key value pairs for the supplied UUID.
    *
    * @param uuid
    * @param entityName
    * @return
    */
  def get(uuid:String, entityName:String) : Option[Map[String, String]] = {
    val stmt = session.prepare(s"SELECT * FROM $entityName where rowkey = ? ALLOW FILTERING")
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
    val stmt = session.prepare(s"SELECT * FROM $entityName where rowkey = ?")
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
    val stmt = session.prepare(s"SELECT * FROM $entityName where $idxColumn = ?")
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
    val stmt = session.prepare(s"SELECT $propertyName FROM $entityName where $idxColumn = ?")
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
    val stmt = session.prepare(s"SELECT $propertyName FROM $entityName where rowkey = ?")
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
  def putBatch(entityName: String, batch: Map[String, Map[String, String]], removeNullFields: Boolean) =
    throw new RuntimeException("No supported")

  /**
    * Store the supplied map of properties as separate columns in cassandra.
    */
  def put(uuid: String, entityName: String, keyValuePairs: Map[String, String], removeNullFields: Boolean) = {
    val placeHolders = ",?" * keyValuePairs.size
    val insertCQL = s"INSERT INTO $entityName (rowkey," + keyValuePairs.keySet.mkString(",") + ") VALUES (?" + placeHolders + ")"
    val statement = session.prepare(insertCQL)
    val boundStatement = if(keyValuePairs.size == 1){
      statement.bind(Array(uuid, keyValuePairs.values.head))
    } else {
      val values = Array(uuid) ++ keyValuePairs.values.toArray[String]
      statement.bind(values:_*)
    }
    val future = session.execute(boundStatement)
    uuid
  }

  /**
    * Store the supplied property value in the column
    */
  def put(uuid: String, entityName: String, propertyName: String, propertyValue: String, removeNullFields: Boolean) = {
    val insertCQL = s"INSERT INTO $entityName (rowkey, $propertyName) VALUES (?,?)"
    val statement = session.prepare(insertCQL)
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




  }

  /**
    * Select fields from rows and pass to the supplied function.
    */
  def selectRows(rowkeys:Seq[String], entityName:String, fields:Seq[String], proc:((Map[String,String])=>Unit)) : Unit =
    throw new RuntimeException("No supported")


  def shutdown = throw new RuntimeException("Not supported")

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
