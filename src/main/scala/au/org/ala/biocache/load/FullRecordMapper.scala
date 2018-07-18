package au.org.ala.biocache.load

import au.org.ala.biocache.Config
import au.org.ala.biocache.model._
import au.org.ala.biocache.poso.POSO
import au.org.ala.biocache.processor.{Processor, Processors}
import au.org.ala.biocache.util.Json
import au.org.ala.biocache.vocab.AssertionCodes
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * This object maps the data from key value pairs into the FullRecord object
  * which represents a record.
  */
object FullRecordMapper {
  val logger = LoggerFactory.getLogger("FullRecordMapper")
  val entityName = "occ"
  val qualityAssertionColumn = "qualityAssertion"
  val userQualityAssertionColumn = "userQualityAssertion"
  val userAssertionStatusColumn = "userAssertionStatus"
  val geospatialDecisionColumn = "geospatiallyKosher"
  val taxonomicDecisionColumn = "taxonomicallyKosher"
  val userVerifiedColumn = "userVerified"
  val locationDeterminedColumn = "locationDetermined"
  val defaultValuesColumn = "defaultValuesUsed"
  val dateDeletedColumn = "dateDeleted"
  val lastUserAssertionDateColumn = "lastUserAssertionDate"
  val environmentalLayersColumn = "el" + Config.persistenceManager.fieldDelimiter + "p"
  val contextualLayersColumn = "cl" + Config.persistenceManager.fieldDelimiter + "p"
  val deletedColumn = "deleted"
  val miscPropertiesColumn = "miscProperties"
  val firstLoadedColumn = "firstLoaded"
  val alaModifiedColumn = "lastModifiedTime"
  val alaModifiedColumnP = "lastModifiedTime" + Config.persistenceManager.fieldDelimiter + "p"
  val geospatialQa = "loc"
  val taxonomicalQa = "class"
  val queryAssertionColumn = "queryAssertions" + Config.persistenceManager.fieldDelimiter + "p"

  val qaFields = Processors.processorMap.values.map(processor => markAsQualityAssertion(processor.asInstanceOf[Processor].getName))

  /**
    * Convert a full record to a map of properties
    */
  def fullRecord2Map(fullRecord: FullRecord, version: Version): scala.collection.mutable.Map[String, String] = {
    val properties = scala.collection.mutable.Map[String, String]()
    fullRecord.objectArray.foreach(poso => {
      val map = FullRecordMapper.mapObjectToProperties(poso, version)
      //add all to map
      properties ++= map
    })

    //add the special cases to the map
    if (fullRecord.miscProperties != null && !fullRecord.miscProperties.isEmpty && version == Raw) {
      properties.put(miscPropertiesColumn, Json.toJSON(fullRecord.miscProperties)) //store them as JSON array
    }
    if (fullRecord.firstLoaded != null && !fullRecord.firstLoaded.isEmpty && version == Raw) {
      properties.put(firstLoadedColumn, fullRecord.firstLoaded)
    }
    if (fullRecord.dateDeleted != null && !fullRecord.dateDeleted.isEmpty && version == Raw) {
      properties.put(dateDeletedColumn, fullRecord.firstLoaded)
    }
    if (fullRecord.el != null && !fullRecord.el.isEmpty && version == Processed) {
      properties.put(environmentalLayersColumn, Json.toJSON(fullRecord.el)) //store them as JSON array
    }
    if (fullRecord.cl != null && !fullRecord.cl.isEmpty && version == Processed) {
      properties.put(contextualLayersColumn, Json.toJSON(fullRecord.cl)) //store them as JSON array
    }
    properties.put("rowKey", fullRecord.rowKey)
    properties.put(FullRecordMapper.defaultValuesColumn, fullRecord.defaultValuesUsed.toString)
    properties.put(FullRecordMapper.locationDeterminedColumn, fullRecord.locationDetermined.toString)
    properties.put(FullRecordMapper.geospatialDecisionColumn, fullRecord.geospatiallyKosher.toString)
    properties.put(FullRecordMapper.taxonomicDecisionColumn, fullRecord.taxonomicallyKosher.toString)

    if (fullRecord.lastModifiedTime != "") {
      properties.put(FullRecordMapper.markNameBasedOnVersion(FullRecordMapper.alaModifiedColumn, version), fullRecord.lastModifiedTime)
    }
    properties
  }

  /**
   * for each field in the definition, check if there is a value to write
   * Change to use the toMap method of a Mappable
   */
  def mapObjectToProperties(anObject: AnyRef, version:Version = Raw): Map[String, String] = {
    anObject match {
      case p: POSO => {
        p.toMap.map { case (key, value) => (markNameBasedOnVersion(key, version) -> value) }
      }
      case _ => throw new Exception("Unrecognised object. Object is not a Mappable or a POSO. Class : " + anObject.getClass.getName)
    }
  }

  /**
   * changes the name based on the version
   */
  def markNameBasedOnVersion(name:String, version:Version) = version match {
    case Processed => markAsProcessed(name)
    case Consensus => markAsConsensus(name)
    case _ => name
  }

  /**
   * Set the property on the correct model object
   */
  def mapPropertiesToObject(anObject: POSO, map: Map[String, String]) =
    map.foreach{ case (key, value) => anObject.setProperty(key, value) }

  /**
   * Sets the properties on the supplied object based on 2 maps
   * <ol>
   * <li>Map of source names to values</li>
   * <li>Map of source names to target values</li>
   * </ol>
   */
  def mapmapPropertiesToObject(poso:POSO, valueMap:scala.collection.Map[String,Object], targetMap:scala.collection.Map[String,String]){

    valueMap.keys.foreach(sourceName => {
      //get the target name
      val targetName = targetMap.getOrElse(sourceName, "")
      if (targetName != "") {
        val value = valueMap.get(sourceName)
        //get the setter method
        if (value.isDefined && value.get != null) {
          poso.setProperty(targetName, value.get.toString)
        }
      }
    })
  }

  /**
   * Create a record from a array of tuple properties
   */
  def createFullRecord(rowKey: String, fieldTuples: Array[(String, String)], version: Version): FullRecord = {
    val fieldMap = Map(fieldTuples map {
      s => (s._1, s._2)
    }: _*)
    createFullRecord(rowKey, fieldMap, version)
  }

  /**
   * Creates an FullRecord from the map of properties
   */
  def createFullRecord(rowKey: String, fields: scala.collection.Map[String, String], version: Version): FullRecord = {

    val fullRecord = new FullRecord
    fullRecord.rowKey = rowKey
    fullRecord.lastModifiedTime = fields.getOrElse(markNameBasedOnVersion(alaModifiedColumn, version), "")
    fullRecord.firstLoaded = fields.getOrElse(firstLoadedColumn, "")

    fullRecord.setRawFieldsWithMapping(fields)
    fields.keySet.foreach { fieldName =>
      //ascertain which term should be associated with which object

      val fieldValue = fields.get(fieldName) match {
        case Some(null) => ""
        case _ => fields.getOrElse(fieldName, "").trim
      }

      //only set the value if it is no null or empty string
      if (fieldValue != "") {
        fieldName match {
          case it if (it.toLowerCase == qualityAssertionColumn.toLowerCase) => {} //ignore ?????
          case it if isQualityAssertion(it) => {
            //load the QA field names from the array
            if (fieldValue != "true" && fieldValue != "false") {
              //parses an array of integers
              val codeBuff = new ArrayBuffer[String]
              //Add the assertions that already exist
              if (fullRecord.assertions != null) {
                codeBuff ++= fullRecord.assertions
              }
              Json.toIntArray(fieldValue).foreach(code => {
                val retrievedCode = AssertionCodes.getByCode(code)
                if (!retrievedCode.isEmpty) {
                  codeBuff += retrievedCode.get.getName
                }
              })
              fullRecord.assertions = codeBuff.toArray
            }
          }
          case it if (it.toLowerCase == miscPropertiesColumn.toLowerCase) => {
            if (version == Raw) {
              fullRecord.miscProperties = Json.toJavaMap(fieldValue).asInstanceOf[java.util.Map[String, String]]
            }
          }
          case it if version == Raw && ("class".equals(it.toLowerCase) || "clazz".equals(it.toLowerCase) || "classs".equals(it.toLowerCase)) => fullRecord.classification.classs = fieldValue
          case it if userVerifiedColumn.equalsIgnoreCase(it) => fullRecord.userVerified = "true".equals(fieldValue)
          case it if taxonomicDecisionColumn.equalsIgnoreCase(it) => fullRecord.taxonomicallyKosher = "true".equals(fieldValue)
          case it if geospatialDecisionColumn.equalsIgnoreCase(it) => fullRecord.geospatiallyKosher = "true".equals(fieldValue)
          case it if defaultValuesColumn.equalsIgnoreCase(it) => fullRecord.defaultValuesUsed = "true".equals(fieldValue)
          case it if locationDeterminedColumn.equalsIgnoreCase(it) => fullRecord.locationDetermined = "true".equals(fieldValue)
          case it if deletedColumn.equalsIgnoreCase(it) => fullRecord.deleted = "true".equals(fieldValue)
          case it if dateDeletedColumn.equalsIgnoreCase(it) => fullRecord.dateDeleted
          case it if lastUserAssertionDateColumn.equalsIgnoreCase(fieldName) => fullRecord.setLastUserAssertionDate(fieldValue)
          case it if userAssertionStatusColumn.equalsIgnoreCase(fieldName) => fullRecord.setUserAssertionStatus(fieldValue)
          case it if userQualityAssertionColumn.equalsIgnoreCase(fieldName) => fullRecord.setUserQualityAssertion(fieldValue)
          case it if version == Processed && isProcessedValue(fieldName) => {
            val success = fullRecord.setProperty(removeSuffix(fieldName), fieldValue)
            if (!success) {
              logger.error("Unable to set " + fieldName + " with value '" + fieldValue + "' for record with rowkey: " + rowKey)
            }
          }
          case it if version == Raw && fullRecord.hasProperty(fieldName) => {
            val success = fullRecord.setProperty(fieldName, fieldValue)
            if (!success) {
              logger.error("Unable to set " + fieldName + " with value '" + fieldValue + "' for record with rowkey: " + rowKey)
            }
          }
          case it if version == Raw && !isProcessedValue(fieldName) => {
            fullRecord.miscProperties.put(fieldName, fieldValue)
          }
          case _ => {
            //properties for raw will fall through when constructing a processed version
            //and vice versa - expected behaviour, do nothing
            //logger.debug("Field not recognised or supported " + fieldName + " with value '" + fieldValue + "' for record with rowkey: " + rowKey)
          }
        }
      }
    }
    fullRecord
  }

  /** FIXME */
  def fieldDelimiter = Config.persistenceManager.fieldDelimiter

  /** Remove the suffix indicating the version of the field */
  def removeSuffix(name: String): String = name.substring(0, name.length - 2)

  /** Is this a "processed" value? */
  def isProcessedValue(name: String): Boolean = name endsWith fieldDelimiter + "p"

  /** Is this a "consensus" value? */
  def isConsensusValue(name: String): Boolean = name endsWith fieldDelimiter + "c"

  /** Is this a "consensus" value? */
  def isQualityAssertion(name: String): Boolean = name endsWith fieldDelimiter + "qa"

  /** Add a suffix to this field name to indicate version type */
  def markAsProcessed(name: String): String = name + fieldDelimiter + "p"

  /** Add a suffix to this field name to indicate version type */
  def markAsConsensus(name: String): String = name + fieldDelimiter + "c"

  /** Add a suffix to this field name to indicate quality assertion field */
  def markAsQualityAssertion(name: String): String = name + fieldDelimiter + "qa"

  /** Remove the quality assertion marker */
  def removeQualityAssertionMarker(name: String): String = name.dropRight(3)
}