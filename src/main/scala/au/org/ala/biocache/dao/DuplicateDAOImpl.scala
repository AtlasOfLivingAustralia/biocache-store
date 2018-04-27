package au.org.ala.biocache.dao

import au.org.ala.biocache.Config
import au.org.ala.biocache.util.BiocacheConversions
import org.slf4j.LoggerFactory
import com.google.inject.Inject
import com.fasterxml.jackson.databind.ObjectMapper
import au.org.ala.biocache.persistence.PersistenceManager
import au.org.ala.biocache.model.DuplicateRecordDetails

class DuplicateDAOImpl extends DuplicateDAO {

  import BiocacheConversions._

  protected val logger = LoggerFactory.getLogger("DuplicateDAO")
  @Inject
  var persistenceManager: PersistenceManager = _

  val mapper = new ObjectMapper
  val lastRunRowKey = "DDLastRun"

  val DUPLICATES_ENTITY = "duplicates"
  val OCC_DUPLICATES_ENTITY = "occ_duplicates"

  def saveDuplicate(primaryRecord:DuplicateRecordDetails) : Unit = {

    val stringValue = mapper.writeValueAsString(primaryRecord)

    Config.persistenceManager.put(
      primaryRecord.uuid,
      OCC_DUPLICATES_ENTITY,
      "value",
      stringValue,
      true,
      false
    )

    primaryRecord.duplicates.foreach { duplicate =>
      val duplicateAsString = mapper.writeValueAsString(duplicate)
      Config.persistenceManager.put(
        duplicate.uuid,
        OCC_DUPLICATES_ENTITY,
        "value",
        stringValue,
        true,
        false
      )
    }

    Config.persistenceManager.put(
      primaryRecord.taxonConceptLsid + "|" + primaryRecord.year + "|" + primaryRecord.month + "|" + primaryRecord.day,
      DUPLICATES_ENTITY,
      "value",
      stringValue,
      true,
      false
    )
  }

  def deleteObsoleteDuplicate(uuid:String){
    val duplicate = getDuplicateInfo(uuid)
    if (duplicate.isDefined){
      logger.info("Deleting " + duplicate.get.getRowKey() + " - " + uuid)
      //now construct the row key for the "duplicates" column family
      val otherKey = duplicate.get.taxonConceptLsid+ "|" + duplicate.get.year + "|" + duplicate.get.month + "|" + duplicate.get.day
      persistenceManager.delete(uuid, OCC_DUPLICATES_ENTITY)
      //now delete the column
      persistenceManager.deleteColumns(otherKey, DUPLICATES_ENTITY, uuid)
    }
  }

  /**
    * Retrieve duplicate details for the provided UUID.
    *
    * @param uuid
    * @return
    */
  def getDuplicateInfo(uuid:String) : Option[DuplicateRecordDetails] = {
    val stringValue = persistenceManager.get(uuid, OCC_DUPLICATES_ENTITY, "value")
    if (stringValue.isDefined){
      Some(mapper.readValue[DuplicateRecordDetails](stringValue.get, classOf[DuplicateRecordDetails]))
    } else {
      None
    }
  }

  /**
   * Returns the existing duplicates for the supplied species and date information.
   *
   * Will allow incremental checks for records that have changed...
   */
  def getDuplicatesFor(lsid:String, year:String, month:String, day:String) : List[DuplicateRecordDetails] ={
    def kvpMap = persistenceManager.get(lsid + "|" + year + "|" + month + "|" + day, DUPLICATES_ENTITY)
    if (kvpMap.isDefined){
      def buf = new scala.collection.mutable.ArrayBuffer[DuplicateRecordDetails]()
      kvpMap.get.foreach { case (key,value) =>
        buf +=  mapper.readValue[DuplicateRecordDetails](value, classOf[DuplicateRecordDetails])
      }
      buf.toList
    } else {
      List()
    }
  }

  /**
   * Returns the last time that the duplication detection was run.
   */
  def getLastDuplicationRun():Option[String] ={
    persistenceManager.get(lastRunRowKey, DUPLICATES_ENTITY, lastRunRowKey)
  }
  /**
   * Updates the last duplication detection run with the supplied date.
   */
  override def setLastDuplicationRun(date:java.util.Date) {
    persistenceManager.put(lastRunRowKey, DUPLICATES_ENTITY, lastRunRowKey, date, true, false)
  }
}
