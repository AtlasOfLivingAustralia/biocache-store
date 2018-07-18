package au.org.ala.biocache.persistence

import scala.collection.mutable.{HashMap, ListBuffer}
import au.org.ala.biocache.util.Json
import java.util.UUID
import java.util.concurrent.Executor

import au.org.ala.biocache.dao.{OccurrenceDAO, OccurrenceDAOImpl}
import au.org.ala.biocache.index.{IndexDAO, SolrIndexDAO}
import com.datastax.driver.core.ResultSet
import com.google.common.util.concurrent.FutureCallback
import org.slf4j.LoggerFactory

class MockPersistenceManager extends PersistenceManager {

  val logger = LoggerFactory.getLogger("MockPersistenceManager")

  override def toString = mockStore.toString

  private val mockStore = new HashMap[String, HashMap[String, HashMap[String, String]]]

  //QA store has a composite key of rowkey, userId, qa type
  private val qaMockStore = new HashMap[(String,String,String), Map[String, String]]

  def clear = mockStore.clear

  def get(uuid: String, entityName: String, propertyName: String) = {
    logger.debug(s"Get for $uuid -  $entityName - $propertyName")
    val entityMap = mockStore.getOrElseUpdate(entityName, HashMap[String, HashMap[String, String]]())
    entityMap.get(uuid) match {
      case Some(map) => map.get(propertyName)
      case None => None
    }
  }

  def get(uuid: String, entityName: String) = {
    logger.debug(s"Get for $uuid -  $entityName")
    val entityMap = mockStore.getOrElseUpdate(entityName, HashMap[String, HashMap[String, String]]())
    entityMap.get(uuid) match {
      case Some(x) => Some(x.toMap)
      case None => None
    }
  }

  def getSelected(uuid: String, entityName: String, propertyNames: Seq[String]): Option[Map[String, String]] = {
    throw new RuntimeException("not implemented yet")
  }

  def pageOverColumnRange(entityName: String, proc: ((String, Map[String, String]) => Boolean), startUuid: String = "", endUuid: String = "", pageSize: Int = 1000, startColumn: String = "", endColumn: String = "") =
    throw new RuntimeException("not implemented yet")

  def getColumnsWithTimestamps(uuid: String, entityName: String): Option[Map[String, Long]] =
    throw new RuntimeException("not implemented yet")

  def getByIndex(uuid: String, entityName: String, idxColumn: String) =
    throw new RuntimeException("not implemented yet")

  def getAllByIndex(rowkey:String, entityName:String, idxColumn:String) : Seq[Map[String,String]] = {

    if(entityName == "qa"){
      val setOfKeys = qaMockStore.keySet.filter(_._1 == rowkey)
      val list = new ListBuffer[Map[String,String]]
      setOfKeys.foreach {
        list += qaMockStore.get(_).get
      }
      list
    } else {
      val keyedData = mockStore.get(entityName)
      if(keyedData.isEmpty) return List[Map[String,String]]()
      val list = new ListBuffer[Map[String,String]]
      keyedData.get.foreach { case (guid, map) =>
        if(idxColumn =="rowkey" && rowkey == guid){
          list += map.toMap
        } else {
          val value = map.getOrElse(idxColumn, "")
          if(value == rowkey){
            list += map.toMap
          }
        }
      }
      list
    }
  }

  def getByIndex(uuid: String, entityName: String, idxColumn: String, propertyName: String) =
    throw new RuntimeException("not implemented yet")

  def getList[A](uuid: String, entityName: String, propertyName: String, theClass: Class[_]): List[A] = {
    logger.debug(s"Get List for $uuid -  $entityName - $theClass")
    mockStore.get(entityName).get.get(uuid) match {
      case Some(x) => {
        val list = x.get(propertyName)
        if (list.isEmpty)
          List()
        else
          Json.toListWithGeneric(list.get, theClass)
      }
      case None => List()
    }
  }

  def put(uuid: String, entityName: String, propertyName: String, propertyValue: String, newRecord:Boolean, deleteIfNullValue: Boolean) = {
    logger.debug(s"Put for $uuid -  $entityName - $propertyName -  $propertyValue")
    val entityMap = mockStore.getOrElseUpdate(entityName, HashMap(uuid -> HashMap[String, String]()))
    val recordMap = entityMap.getOrElseUpdate(uuid, HashMap[String, String]())
    recordMap.put(propertyName, propertyValue)
    uuid
  }

  def put(uuid: String, entityName: String, keyValuePairs: Map[String, String], newRecord:Boolean, deleteIfNullValue: Boolean) = {
    logger.debug(s"Put for $uuid -  $entityName - $keyValuePairs")
    if(entityName == "qa"){
      qaMockStore.put((uuid,keyValuePairs.getOrElse("userId",""),keyValuePairs.getOrElse("code","")), keyValuePairs)
      uuid
    } else {
      val entityMap = mockStore.getOrElseUpdate(entityName, HashMap(uuid -> HashMap[String, String]()))
      val recordMap = entityMap.getOrElse(uuid, HashMap[String, String]())
      entityMap.put(uuid, (recordMap ++ keyValuePairs).asInstanceOf[HashMap[String, String]])
      uuid
    }
  }

  def putAsync(uuid: String, entityName: String, keyValuePairs: Map[String, String], newRecord:Boolean, deleteIfNullValue: Boolean) = {
    logger.debug(s"Put for $uuid -  $entityName - $keyValuePairs")
    val entityMap = mockStore.getOrElseUpdate(entityName, HashMap(uuid -> HashMap[String, String]()))
    val recordMap = entityMap.getOrElse(uuid, HashMap[String, String]())
    entityMap.put(uuid, (recordMap ++ keyValuePairs).asInstanceOf[HashMap[String, String]])
    uuid
  }

  def putBatch(entityName: String, batch: Map[String, Map[String, String]], newRecord:Boolean, removeNullFields: Boolean) = {

    batch.foreach { case (uuid, keyValuePairs) =>
      logger.debug(s"Put for $uuid -  $entityName - $keyValuePairs")
      val entityMap = mockStore.getOrElseUpdate(entityName, HashMap(uuid -> HashMap[String, String]()))
      val recordMap = entityMap.getOrElse(uuid, HashMap[String, String]())
      entityMap.put(uuid, (recordMap ++ keyValuePairs).asInstanceOf[HashMap[String, String]])
    }
  }

  def putList[A](uuid: String, entityName: String, propertyName: String, newList: Seq[A], theClass: Class[_], newRecord:Boolean, overwrite: Boolean, deleteIfNullValue: Boolean) = {
    logger.debug(s"PutList for $uuid -  $entityName - $propertyName - $newList - $theClass")
    val recordId = {
      if (uuid != null) uuid else UUID.randomUUID.toString
    }
    val entityMap = mockStore.getOrElseUpdate(entityName, HashMap(uuid -> HashMap[String, String]()))
    val recordMap = entityMap.getOrElse(uuid, HashMap[String, String]())
    if (overwrite) {
      val json: String = Json.toJSONWithGeneric(newList)
      recordMap.put(propertyName, json)
    } else {
      val currentList = getList(uuid, entityName, propertyName, theClass).asInstanceOf[List[A]]
      var buffer = new ListBuffer[A]

      currentList.foreach { theObject =>
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
      recordMap.put(propertyName, newJson)
    }
    recordId

  }

  def pageOverAll(entityName: String, proc: (String, Map[String, String]) => Boolean, startUuid: String, endUuid: String, pageSize: Int) = {
    logger.debug(s"pageOverAll for  $entityName - $startUuid - $endUuid")
    val entityMap = mockStore.getOrElseUpdate(entityName, HashMap[String, HashMap[String, String]]())
    entityMap.keySet.foreach(key => {
      if (key.startsWith(startUuid) && !key.startsWith(endUuid)) {
        proc(key, entityMap.getOrElse(key, HashMap[String, String]()).toMap)
      }
    })
    //throw new RuntimeException("not implemented yet")
  }

  def pageOverSelect(entityName: String, proc: (String, Map[String, String]) => Boolean, pageSize: Int, threads:Int, columnName: String*) =
    throw new RuntimeException("not implemented yet")

  def selectRows(uuids: Seq[String], entityName: String, propertyNames: Seq[String], proc: (Map[String, String]) => Unit) =
    throw new RuntimeException("not implemented yet")

  def deleteColumns(uuid: String, entityName: String, columnName: String*) =
    throw new RuntimeException("not implemented yet")

  def delete(uuid: String, entityName: String) = {
    logger.debug(s"delete for  $uuid -  $entityName")
    val entityMap = mockStore.getOrElseUpdate(entityName, HashMap(uuid -> HashMap[String, String]()))
    entityMap.remove(uuid)
  }

  def delete(properties: Map[String, String], entityName: String) = {
    if(entityName =="qa"){
      qaMockStore.remove(properties.getOrElse("rowkey", ""),properties.getOrElse("userId", ""), properties.getOrElse("code", ""))
    } else {
      throw new RuntimeException("not implemented yet")
    }
  }

  def shutdown = mockStore.clear

  def pageOverLocal(entityName: String, proc: (String, Map[String, String]) => Boolean, threads: Int, columns:Array[String]): Int = {
    throw new RuntimeException("Not implemented yet!!!")
  }

  def createSecondaryIndex(entityName:String, indexFieldName:String, threads:Int) = 0

  /**
    * Page over all records using an indexed field
    *
    * @param entityName
    * @param proc
    * @param indexedField
    * @param indexedFieldValue
    * @param threads
    */
  override def pageOverIndexedField(entityName: String, proc: (String, Map[String, String]) => Boolean, indexedField: String, indexedFieldValue: String, threads: Int, localOnly: Boolean): Int = {
    1
  }

  def rowKeyExists(rowKey:String, entityName:String) : Boolean = false

  /**
    * Page over the records that are local to this node.
    *
    * @param entityName
    * @param proc
    * @param threads
    * @return
    */
  override def pageOverLocal(entityName: String, proc: (String, Map[String, String], String) => Boolean, threads: Int, columns: Array[String]): Int = {
    1
  }
}