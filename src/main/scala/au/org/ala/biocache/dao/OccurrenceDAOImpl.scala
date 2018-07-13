package au.org.ala.biocache.dao

import java.io.OutputStream
import java.util.Date

import au.org.ala.biocache._
import au.org.ala.biocache.index.{IndexDAO, IndexFields}
import au.org.ala.biocache.load.{DownloadMedia, FullRecordMapper}
import au.org.ala.biocache.model._
import au.org.ala.biocache.persistence.PersistenceManager
import au.org.ala.biocache.processor.Processors
import au.org.ala.biocache.util.{BiocacheConversions, Json}
import au.org.ala.biocache.vocab.{AssertionCodes, AssertionStatus, ErrorCode}
import com.google.inject.Inject
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * A DAO for accessing occurrences.
 */
class OccurrenceDAOImpl extends OccurrenceDAO {

  import JavaConversions._

  protected val logger = LoggerFactory.getLogger("OccurrenceDAO")
  @Inject
  var persistenceManager: PersistenceManager = _
  @Inject
  var indexDAO: IndexDAO = _

  import BiocacheConversions._

  val ROW_KEY = "rowkey"
  val UUID = "uuid"


  val elpattern = """el[0-9]+""".r
  val clpattern = """cl[0-9]+""".r

  /**
    * These are properties that shouldnt be updated as they are provided and
    * set by down stream processes.
    */
  val protectedProperties = List(
    "duplicationStatus_p",
    "associatedOccurrences_p",
    "outlierForLayers_p"
  )

  /**
   * Gets the map for a record based on searching the index for new and old ids
   */
  def getMapFromIndex(value:String):Option[Map[String,String]]={
    persistenceManager.getByIndex(value, entityName, UUID) match {
      case None => persistenceManager.getByIndex(value, entityName, "portalId") //legacy record ID
      case Some(map) => Some(map)
    }
  }

  /**
   * Get an occurrence with rowKey
   */
  def getByRowKey(rowKey:String, includeSensitive:Boolean): Option[FullRecord] ={
    getByRowKey(rowKey, Raw, includeSensitive)
  }

 /**
  * Get all the versions based on a row key
  */
  def getAllVersionsByRowKey(rowKey:String, includeSensitive:Boolean=false): Option[Array[FullRecord]] ={
    val map = persistenceManager.get(rowKey, entityName)
    if (map.isEmpty) {
      None
    } else {
      // the versions of the record
      val raw = FullRecordMapper.createFullRecord(rowKey, map.get, Raw)
      val processed = FullRecordMapper.createFullRecord(rowKey, map.get, Processed)
      val consensus = FullRecordMapper.createFullRecord(rowKey, map.get, Consensus)
      if (includeSensitive && raw.occurrence.originalSensitiveValues != null) {
        FullRecordMapper.mapPropertiesToObject(raw, raw.occurrence.originalSensitiveValues)
      }
      //pass all version to the procedure, wrapped in the Option
      Some(Array(raw, processed, consensus))
    }
  }

  def getRawProcessedByRowKey(rowKey: String): Option[Array[FullRecord]] = {
    val map = persistenceManager.get(rowKey, entityName)
    if (map.isEmpty) {
      None
    } else {
      // the versions of the record
      val raw = FullRecordMapper.createFullRecord(rowKey, map.get, Raw)
      val processed = FullRecordMapper.createFullRecord(rowKey, map.get, Processed)
      Some(Array(raw, processed))
    }
  }

  /**
    * Get the supplied version based on a rowKey
    */
  def getByRowKey(rowKey: String, version: Version, includeSensitive: Boolean = false): Option[FullRecord] = {
    val propertyMap = persistenceManager.get(rowKey, entityName)
    if (propertyMap.isEmpty) {
      None
    } else {
      val record = FullRecordMapper.createFullRecord(rowKey, propertyMap.get, version)
      if (includeSensitive && record.occurrence.originalSensitiveValues != null && version == Versions.RAW)
        FullRecordMapper.mapPropertiesToObject(record, record.occurrence.originalSensitiveValues)
      Some(record)
    }
  }

  /**
    * Get an occurrence, specifying the version of the occurrence.
    */
  def getByUuid(uuid: String, version: Version, includeSensitive: Boolean = false): Option[FullRecord] = {
    //get the row key from the supplied uuid
//    val rowKey = getRowKeyFromUuid(uuid)
//    if(rowKey.isDefined){
      getByRowKey(uuid, version,includeSensitive)
//    } else {
//      None
//    }
  }

  /**
    * Create or retrieve the UUID for this record. The uniqueID should be a
    * has of properties that provides a unique ID for the record within
    * the dataset.
    *
    * This method has been changed so that it queries the existing occ record
    * to see if it exists.  We wish for uuids to be persistent between loads.
    *
    * Returns uuid and true when a new uid was created
    */
  def createOrRetrieveUuid(uniqueID: String): (String, Boolean) = {
    //look up by index
    val recordUUID = getUUIDForUniqueID(uniqueID)
    if (recordUUID.isEmpty) {
      val newUuid = createUuid
      //The uuid will be added when the record is inserted
      persistenceManager.put(uniqueID, "occ_uuid", "value", newUuid, true, false)
      (newUuid, true)
    } else {
      (recordUUID.get, false)
    }
  }

  /**
    * Creates a unique key for this record using the unique terms for this data resource.
    *
    * @param dataResourceUid
    * @param identifyingTerms
    * @param stripSpaces
    * @return
    */
  def createUniqueID(dataResourceUid:String, identifyingTerms:Seq[String], stripSpaces:Boolean=false) : String = {
    val uniqueId = (List(dataResourceUid) ::: identifyingTerms.toList).mkString("|").trim
    if(stripSpaces)
      uniqueId.replaceAll("\\s","")
    else
      uniqueId
  }

  /**
   * Retrieve the UUID for a unique record ID
   *
   * @param uniqueID
   * @return
   */
  def getUUIDForUniqueID(uniqueID: String) = persistenceManager.get(uniqueID, "occ_uuid", "value")

  /**
    * Writes the supplied field values to the writer.  The Writer specifies the format in which the record is
    * written.
    */
  def writeToRecordWriter(writer: RecordWriter, rowKeys: Array[String], fields: Array[String], qaFields: Array[String], includeSensitive: Boolean = false, includeMisc: Boolean = false, miscFields: Array[String] = null, dataToInsert: java.util.Map[String, Array[String]] = null): Array[String] = {
    //get the codes for the qa fields that need to be included in the download
    //TODO fix this in case the value can't be found
    val mfields = fields.toBuffer
    val codes = qaFields.map(value => AssertionCodes.getByName(value).get.getCode)
    val firstEL = fields.find(value => {
      elpattern.findFirstIn(value).nonEmpty
    })
    val firstCL = fields.find(value => {
      clpattern.findFirstIn(value).nonEmpty
    })
    val firstMisc = fields.find(value => {
      IndexFields.storeMiscFields.contains(value)
    })
    //user_assertions is boolean in SOLR, FullRecordMapper.userQualityAssertionColumn in Cassandra.
    //because this is a full list it is more useful to have the assertion contents in this requested field.
    val userAssertions = fields.find(value => {
      "user_assertions".equals(value)
    })

    if (firstEL.isDefined)
      mfields += "el" + Config.persistenceManager.fieldDelimiter + "p"
    if (firstCL.isDefined)
      mfields += "cl" + Config.persistenceManager.fieldDelimiter + "p"
    if (includeSensitive)
      mfields += "originalSensitiveValues"
    if (firstMisc.isDefined || includeMisc)
      mfields += FullRecordMapper.miscPropertiesColumn
    mfields ++= FullRecordMapper.qaFields

    //user assertions requires additional columns that may or may not already be requested
    var addedUserQAColumn = false
    var addedRowKey = false
    if (userAssertions.isDefined) {
      if (!mfields.contains(FullRecordMapper.userQualityAssertionColumn)) {
        mfields += FullRecordMapper.userQualityAssertionColumn
        mfields += FullRecordMapper.userAssertionStatusColumn
        addedUserQAColumn = true
      }
    }
    if (userAssertions.isDefined || (dataToInsert != null && dataToInsert.size > 0)) {
      if (!mfields.contains(ROW_KEY)) {
        mfields += ROW_KEY
        addedRowKey = true
      }
    }

    val newMiscFields: ListBuffer[String] = ListBuffer[String]()
    if (miscFields != null) miscFields.foreach(field => newMiscFields += field)

    persistenceManager.selectRows(rowKeys, entityName, mfields, { fieldMap =>
      val array = scala.collection.mutable.ArrayBuffer[String]()
      val sensitiveMap: scala.collection.Map[String, String] = if (includeSensitive) Json.toStringMap(fieldMap.getOrElse("originalSensitiveValues", "{}")) else Map()
      val elMap = if (firstEL.isDefined) Json.toStringMap(fieldMap.getOrElse("el" + Config.persistenceManager.fieldDelimiter + "p", "{}")) else Map[String, String]()
      val clMap = if (firstCL.isDefined) Json.toStringMap(fieldMap.getOrElse("cl" + Config.persistenceManager.fieldDelimiter + "p", "{}")) else Map[String, String]()
      val miscMap = if (firstMisc.isDefined || includeMisc) Json.toStringMap(fieldMap.getOrElse(FullRecordMapper.miscPropertiesColumn, "{}")) else Map[String, String]()
      fields.foreach { field =>
        val fieldValue = field match {
          case a if elpattern.findFirstIn(a).nonEmpty => elMap.getOrElse(a, "")
          case a if clpattern.findFirstIn(a).nonEmpty => clMap.getOrElse(a, "")
          case a if firstMisc.isDefined && IndexFields.storeMiscFields.contains(a) => miscMap.getOrElse(a, "")
          case a if userAssertions.isDefined && "user_assertions".equals(a) => {
            if ("true".equals(fieldMap.getOrElse(FullRecordMapper.userQualityAssertionColumn, "false")))
              getUserAssertionsString(fieldMap.getOrElse(ROW_KEY,""))
            else
              ""
          }
          case _ => if(includeSensitive) sensitiveMap.getOrElse(field, getHackValue(field,fieldMap)) else getHackValue(field,fieldMap)
        }

        //do not add columns not requested
        if (!(addedRowKey && ROW_KEY.equals(field)) &&
          !(addedUserQAColumn && FullRecordMapper.userQualityAssertionColumn.equals(field))) {
          // if(includeSensitive) sensitiveMap.getOrElse(field, getHackValue(field,fieldMap))else getHackValue(field,fieldMap)
          //Create a MS Excel compliant CSV file thus field with delimiters are quoted andm embedded quotes are escaped
          array += fieldValue
        }

      }

      //add additional columns
      if (dataToInsert != null && dataToInsert.size > 0) {
        val data = dataToInsert.get(fieldMap.getOrElse(ROW_KEY, ""))
        if (data != null) {
          data.foreach { v =>
            array += v
          }
        }
      }

      //now handle the QA fields
      val failedCodes = getErrorCodes(fieldMap)
      //work way through the codes and add to output
      codes.foreach(code => {
        array += (failedCodes.contains(code)).toString
      })
      //include all misc fields
      if (includeMisc) {
        //match miscFields order
        newMiscFields.foreach { field =>
          array += miscMap.getOrElse(field, "")
        }
        //unmatched
        miscMap.foreach { m =>
          if (!newMiscFields.contains(m._1)) {
            newMiscFields += m._1
            array += m._2
          }
        }
      }
      writer.write(array.toArray)
    })

    newMiscFields.toArray
  }

  /**
    * Write to stream in a delimited format (CSV).
    */
  def writeToStream(outputStream: OutputStream, fieldDelimiter: String, recordDelimiter: String,
                    rowKeys: Array[String], fields: Array[String], qaFields: Array[String], includeSensitive: Boolean = false) {
    //get the codes for the qa fields that need to be included in the download
    //TODO fix this in case the value can't be found
    val mfields = scala.collection.mutable.ArrayBuffer[String]()
    mfields ++= fields
    val codes = qaFields.map(value => AssertionCodes.getByName(value).get.getCode)
    val firstEL = fields.find(value => {
      elpattern.findFirstIn(value).nonEmpty
    })
    val firstCL = fields.find(value => {
      clpattern.findFirstIn(value).nonEmpty
    })
    var extraFields = Array[String]()
    if (firstEL.isDefined)
      mfields += "el" + Config.persistenceManager.fieldDelimiter + "p"
    if (firstCL.isDefined)
      mfields += "cl" + Config.persistenceManager.fieldDelimiter + "p"
    if (includeSensitive)
      mfields += "originalSensitiveValues"
    mfields ++= FullRecordMapper.qaFields

    //val fieldsToQuery = if(includeSensitive) fields ++ FullRecordMapper.qaFields ++ Array("originalSensitiveValues") else fields ++ FullRecordMapper.qaFields
    persistenceManager.selectRows(rowKeys, entityName, mfields, { fieldMap =>
      val sensitiveMap: scala.collection.Map[String, String] = if (includeSensitive) Json.toStringMap(fieldMap.getOrElse("originalSensitiveValues", "{}")) else Map()
      val elMap = if (firstEL.isDefined) Json.toStringMap(fieldMap.getOrElse("el" + Config.persistenceManager.fieldDelimiter + "p", "{}")) else Map[String, String]()
      val clMap = if (firstCL.isDefined) Json.toStringMap(fieldMap.getOrElse("cl" + Config.persistenceManager.fieldDelimiter + "p", "{}")) else Map[String, String]()
      fields.foreach(field => {
        val fieldValue = field match {
          case a if elpattern.findFirstIn(a).nonEmpty => elMap.getOrElse(a, "")
          case a if clpattern.findFirstIn(a).nonEmpty => clMap.getOrElse(a, "")
          case _ => if (includeSensitive) sensitiveMap.getOrElse(field, getHackValue(field, fieldMap)) else getHackValue(field, fieldMap)
        }
        // if(includeSensitive) sensitiveMap.getOrElse(field, getHackValue(field,fieldMap))else getHackValue(field,fieldMap)
        //Create a MS Excel compliant CSV file thus field with delimiters are quoted and embedded quotes are escaped

        if (fieldValue.contains(fieldDelimiter) || fieldValue.contains(recordDelimiter) || fieldValue.contains("\""))
          outputStream.write(("\"" + fieldValue.replaceAll("\"", "\"\"") + "\"").getBytes)
        else
          outputStream.write(fieldValue.getBytes)
        outputStream.write(fieldDelimiter.getBytes)
      })
      //now handle the QA fields
      val failedCodes = getErrorCodes(fieldMap)
      //work way through the codes and add to output
      codes.foreach(code => {
        outputStream.write((failedCodes.contains(code)).toString.getBytes)
        outputStream.write(fieldDelimiter.getBytes)
      })
      outputStream.write(recordDelimiter.getBytes)
    })
  }

  /**
    * A temporary HACK to get some of the values for the download that are NOT stored directly
    * TODO REMOVE this Hack
    */
  def getHackValue(field: String, map: Map[String, String]): String = {
    if (FullRecordMapper.geospatialDecisionColumn == field) {
      if ("false" == map.getOrElse(field, ""))
        "Spatially suspect"
      else
        "Spatially valid"
    }
    else if ("outlierForLayers" + Config.persistenceManager.fieldDelimiter + "p" == field) {
      val out = map.getOrElse("outlierForLayers" + Config.persistenceManager.fieldDelimiter + "p", "[]")
      Json.toStringArray(out).length.toString
    }
    else
      map.getOrElse(field, "")
  }

  def getUserAssertionsString(rowKey: String): String = {
    val assertions: List[QualityAssertion] = getUserAssertions(rowKey)
    val string: StringBuilder = new StringBuilder()
    assertions.foreach { assertion =>
      if (assertion != null) {
        if (!string.isEmpty) string.append('|')
        //format as ~ delimited created~name~comment~user
        val comment = {
          if (assertion.comment != null) {
            assertion.comment
          } else {
            ""
          }
        }
        val userDisplayName = {
          if (assertion.userDisplayName != null) {
            assertion.userDisplayName
          } else {
            ""
          }
        }
        val formatted = assertion.created + "~" + assertion.name + "~" + comment.replace('~', '-').replace('\n', ' ') + "~" + userDisplayName.replace('~', '-')
        string.append(formatted.replace('|', '/'))
      }
    }
    string.toString()
  }

  def getErrorCodes(map: Map[String, String]): Array[Integer] = {
    val array: Array[List[Integer]] = FullRecordMapper.qaFields.filter(field => map.get(field).getOrElse("[]") != "[]").toArray.map(field => {
      Json.toListWithGeneric(map.get(field).get, classOf[java.lang.Integer])
    }).asInstanceOf[Array[List[Integer]]]
    if (!array.isEmpty)
      return array.reduceLeft(_ ++ _).toArray
    return Array()
  }

  /**
    * Iterate over all occurrences, passing all versions of FullRecord
    * to the supplied function.
    * Function returns a boolean indicating if the paging should continue.
    *
    * @param proc            , the function to execute.
    * @param dataResourceUid , The data resource to page over.
    */
  def pageOverAllVersions(proc: ((Option[Array[FullRecord]]) => Boolean), dataResourceUid: String, pageSize: Int = 1000) {

    persistenceManager.pageOverIndexedField(entityName, (guid, map) => {
      //retrieve all versions
      val raw = FullRecordMapper.createFullRecord(guid, map, Raw)
      val processed = FullRecordMapper.createFullRecord(guid, map, Processed)
      val consensus = FullRecordMapper.createFullRecord(guid, map, Consensus)
      //pass all version to the procedure, wrapped in the Option
      proc(Some(Array(raw, processed, consensus)))
    }, "dataResourceUid", dataResourceUid, pageSize, false)
  }

  /**
    * Iterate over all occurrences, passing the objects to a function.
    * Function returns a boolean indicating if the paging should continue.
    *
    * @param proc , the function to execute.
    */
  def pageOverAll(version: Version, proc: ((Option[FullRecord]) => Boolean), dataResourceUid: String, pageSize: Int = 1000) {
    persistenceManager.pageOverIndexedField(entityName, (guid, map) => {
      //retrieve all versions
      val fullRecord = FullRecordMapper.createFullRecord(guid, map, version)
      //pass all version to the procedure, wrapped in the Option
      proc(Some(fullRecord))
    }, "dataResourceUid", dataResourceUid, pageSize, false)
  }

  /**
    * Iterate over all occurrences, passing the objects to a function.
    * Function returns a boolean indicating if the paging should continue.
    *
    * @param proc            , the function to execute.
    * @param dataResourceUid , The data resource to process
    */
  def pageOverRawProcessed(proc: (Option[(FullRecord, FullRecord)] => Boolean), dataResourceUid: String, pageSize: Int = 1000, threads: Int = 4) {
    persistenceManager.pageOverIndexedField(entityName, (guid, map) => {
      //retrieve all versions
      val raw = FullRecordMapper.createFullRecord(guid, map, Versions.RAW)
      val processed = FullRecordMapper.createFullRecord(guid, map, Versions.PROCESSED)
      //pass all version to the procedure, wrapped in the Option
      proc(Some(raw, processed))
    }, "dataResourceUid", dataResourceUid, pageSize)
  }


  /**
    * Page over the records local to this node.
    *
    * @param proc
    * @param dataResourceUid
    * @param threads
    * @return
    */
  def pageOverRawProcessedLocal(proc: (Option[(FullRecord, FullRecord)] => Boolean), dataResourceUid: String, threads: Int = 4): Int = {

    if (StringUtils.isNotBlank(dataResourceUid)) {
      persistenceManager.pageOverIndexedField(entityName, (guid, map) => {
        //retrieve all versions
        val raw = FullRecordMapper.createFullRecord(guid, map, Versions.RAW)
        val processed = FullRecordMapper.createFullRecord(guid, map, Versions.PROCESSED)
        //pass all version to the procedure, wrapped in the Option
        proc(Some(raw, processed))
      }, "dataResourceUid", dataResourceUid, threads, localOnly = true)

    } else {
      persistenceManager.pageOverLocal(entityName, (guid, map, tokenRangeIdx) => {
        //retrieve all versions
        val raw = FullRecordMapper.createFullRecord(guid, map, Versions.RAW)
        val processed = FullRecordMapper.createFullRecord(guid, map, Versions.PROCESSED)
        //pass all version to the procedure, wrapped in the Option
        proc(Some(raw, processed))
      }, threads, Array())
    }
  }

//  /**
//   * Iterate over the occurrence which match a condition.
//   * Prevents overhead of processing records that are deleted. Also it is quicker to get a smaller
//   * number of columns.  Thus only get all the columns for record that need to be processed.
//   *
//   * The shouldProcess function should take the map and determine based on conditions whether or not to retrieve the complete record
//   */
//  def conditionalPageOverRawProcessed(proc: (Option[(FullRecord, FullRecord)] => Boolean),
//                                      condition:(Map[String,String]=>Boolean),
//                                      columnsToRetrieve:Array[String],
//                                      dataResourceUID:String = "",
//                                      pageSize: Int = 1000){
//
//    val columns = columnsToRetrieve ++ Array(UUID, ROW_KEY)
//    persistenceManager.pageOverSelect(entityName, (guid, map) => {
//      if(condition(map)){
//        if(map.contains(ROW_KEY)){
//          val recordmap = persistenceManager.get(map.get(ROW_KEY).get,entityName)
//          if(!recordmap.isEmpty){
//            val raw = FullRecordMapper.createFullRecord(guid, recordmap.get, Versions.RAW)
//            val processed = FullRecordMapper.createFullRecord(guid, recordmap.get, Versions.PROCESSED)
//            //pass all version to the procedure, wrapped in the Option
//            proc(Some(raw, processed))
//          }
//        } else {
//          logger.info("Unable to page over records : " +guid)
//        }
//      }
//      true
//    }, "dataResourceUID", dataResourceUID, pageSize, columns: _*)
//  }

  /**
    * Update the version of the occurrence record.
    */
  def addRawOccurrence(fr: FullRecord, deleteIfNullValue: Boolean = false) {

    //add the last load time
    fr.lastModifiedTime = new Date
    if (fr.firstLoaded == null) {
      fr.firstLoaded = fr.lastModifiedTime
    }

    //process the record
    val properties = FullRecordMapper.fullRecord2Map(fr, Versions.RAW)

    //commit
    if (deleteIfNullValue) {
      properties ++= fr.getRawFields().filter { case (k, v) => !properties.isDefinedAt(k) } map { case (k, v) => (k, null) }
    }

    persistenceManager.put(fr.rowKey, entityName, properties.toMap, true, deleteIfNullValue)
  }

  /**
    *
    * @param rowKey
    * @return
    */
  def rowKeyExists(rowKey:String): Boolean = persistenceManager.rowKeyExists(rowKey, "occ")

  /**
    * Update the version of the occurrence record.
    */
  def addRawOccurrenceBatch(fullRecords: Array[FullRecord], removeNullFields: Boolean = false) {
    val batch = scala.collection.mutable.Map[String, Map[String, String]]()
    val batchSecondaryIndex = scala.collection.mutable.Map[String, Map[String, String]]()

    fullRecords.foreach { fr =>
      //process the record
      val properties = FullRecordMapper.fullRecord2Map(fr, Versions.RAW)
      if (removeNullFields) {
        properties ++= fr.getRawFields().filter {
          case (k, v) => !properties.isDefinedAt(k)
        }
          .map {
            case (k, v) => (k, null)
          }
      }
      batch.put(fr.rowKey, properties.toMap)
    }
    //commit
    persistenceManager.putBatch(entityName, batch.toMap, true, removeNullFields)
  }

  /**
    * Download the associated media and update the references in the FR.
    * Returns true if media has been downloaded.
    *
    * @param fr
    */
  def downloadMedia(fr: FullRecord): Boolean = {
    if (fr.occurrence.associatedMedia != null) {
      val filesToImport = DownloadMedia.unpackAssociatedMedia(fr.occurrence.associatedMedia)
      val associatedMediaBuffer = new ArrayBuffer[String]
      filesToImport.foreach(fileToStore =>
        Config.mediaStore.save(fr.rowKey, fr.attribution.dataResourceUid, fileToStore, None) match {
          case Some((filename, filePath)) => associatedMediaBuffer += filePath
          case None => logger.error("Unable to save media: " + fileToStore)
        }
      )
      fr.occurrence.associatedMedia = associatedMediaBuffer.toArray.mkString(";")
      fr.occurrence.images = associatedMediaBuffer.toArray.toArray
      true
    } else {
      false
    }
  }

  /**
   * Update the version of the occurrence record.
   */
  def updateOccurrence(rowKey: String, fullRecord: FullRecord, version: Version) {
    updateOccurrence(rowKey, fullRecord, None, version)
  }

  /**
   * Update the occurrence with the supplied record, setting the correct version.
   * This implementation updates the records and assertions in a single write.
   */
  def updateOccurrence(guid: String, fullRecord: FullRecord, assertions: Option[Map[String,Array[QualityAssertion]]], version: Version) {
    updateOccurrenceBatch(List(
      Map("rowKey" -> guid,
        "oldRecord" -> null,
        "newRecord" -> fullRecord,
        "assertions" -> assertions,
        "version" -> version)
    ))
  }

  /**
   * Update the occurrence with the supplied record, setting the correct version
   */
  def updateOccurrence(guid: String, oldRecord: FullRecord, newRecord: FullRecord,
                       assertions: Option[Map[String,Array[QualityAssertion]]], version: Version) {

    updateOccurrenceBatch(List(
      Map("rowKey" -> guid,
        "oldRecord" -> oldRecord,
        "newRecord" -> newRecord,
        "assertions" -> assertions,
        "version" -> version)
    ))
  }

  /**
   * Update the occurrence with the supplied record, setting the correct version
   */
  def updateOccurrenceBatch(batch: List[Map[String, Object]]) {
    //make list
    val all = scala.collection.mutable.Map[String, Map[String, String]]()

    batch.filter(_ != null).foreach { values =>

      val rowKey = values.get("rowKey").get.asInstanceOf[String]
      val oldRecord = values.get("oldRecord").get.asInstanceOf[FullRecord]
      val version = values.get("version").get.asInstanceOf[Version]
      val newRecord = values.get("newRecord").get.asInstanceOf[FullRecord]
      val assertions = values.get("assertions").get.asInstanceOf[Option[Map[String, Array[QualityAssertion]]]]

      var propertiesToPersist = if(oldRecord != null) {
        //construct a map of properties to write
        val oldproperties = FullRecordMapper.fullRecord2Map(oldRecord, version)
        val properties = FullRecordMapper.fullRecord2Map(newRecord, version)

        //only write changes.........
        var propertiesToPersist = properties.filter {
          case (key, value) => {
            if (protectedProperties.contains(key)){
              false
            } else if( !oldproperties.containsKey(key) && value == "") {
              false
            } else if (oldproperties.contains(key)) {
              val oldValue = oldproperties.get(key).get
              oldValue != value
            } else {
              true
            }
          }
        }
        //check for deleted properties
        val deletedProperties = oldproperties.filter {
          case (key, value) => !protectedProperties.contains(key) && !properties.contains(key)
        }

        propertiesToPersist ++= deletedProperties.map {
          case (key, value) => key -> ""
        }

        propertiesToPersist

      } else {
        FullRecordMapper.fullRecord2Map(newRecord, version)
      }

      val timeCol = FullRecordMapper.markNameBasedOnVersion(FullRecordMapper.alaModifiedColumn, version)

      if (!assertions.isEmpty) {
        initAssertions(newRecord, assertions.get)
        //only add  the assertions if they are different OR the properties to persist contain more than the last modified time stamp
        if (
          oldRecord == null ||
          oldRecord.assertions.toSet != newRecord.assertions.toSet ||
          propertiesToPersist.size > 1  //i.e. theres more than just the timestamp to update
        ) {
          //only add the assertions if they have changed since the last time or the number of records to persist >1
          val checkUserAssertions = oldRecord != null && StringUtils.isNotEmpty(oldRecord.getUserAssertionStatus)

          propertiesToPersist ++= convertAssertionsToMap(rowKey, assertions.get, checkUserAssertions)
          val x = assertions.get.values.filter{!_.isEmpty}.flatten.toList

          propertiesToPersist ++= Map(FullRecordMapper.qualityAssertionColumn ->  Json.toJSONWithGeneric(x))
        }
      }

      //commit to cassandra if changes exist - changes exist if the properties to persist contain more info than the lastModifedTime
      if (!propertiesToPersist.isEmpty && !(propertiesToPersist.size == 1 && propertiesToPersist.getOrElse(timeCol, "") != "")) {
        all.put(rowKey, propertiesToPersist.toMap)
      }
    }

    if (!all.isEmpty) {
      persistenceManager.putBatch(entityName, all.toMap, false, false)
    }
  }

  private def initAssertions(processed:FullRecord, assertions:Map[String, Array[QualityAssertion]]){
    assertions.values.foreach { array =>
      val failedQas = array.filter(_.qaStatus==0).map(_.getName)
      processed.assertions = processed.assertions ++ failedQas
    }
  }

  def doesListContainCode(list: List[QualityAssertion], code: Int) = !list.filter(ua => ua.code == code).isEmpty

  /**
    * Convert the assertions to a map
    * @param rowKey of records to check
    * @param systemAssertions systemAssertions to merge with
    * @param checkUserAssertions whether to check user assertions. Setting this to false has the performance benefit of skipping a DB call.
    */
  def convertAssertionsToMap(rowKey: String, systemAssertions: Map[String, Array[QualityAssertion]], checkUserAssertions:Boolean): Map[String, String] = {
    //if supplied, update the assertions
    val properties = new collection.mutable.ListMap[String, String]

    val userAssertions = if(checkUserAssertions){
      getUserAssertions(rowKey)
    } else {
      List()
    }

    // Updating system assertion, pass in false
    val (userAssertionStatus, trueUserAssertions) = getCombinedUserStatus(false, userAssertions)

    val verified = if (userAssertionStatus == AssertionStatus.QA_VERIFIED || userAssertionStatus == AssertionStatus.QA_CORRECTED) true else false

    val falseUserAssertions = userAssertions.filter { qa =>
      qa.code != AssertionCodes.VERIFIED.code &&
        AssertionStatus.isUserAssertionType(qa.qaStatus) &&
        !trueUserAssertions.exists(a => a.code == qa.code)
    }

    if (verified) {
      //kosher fields are always set to true for verified BUT we still want to store and report the QA's that failed
      var listErrorCodes = new ArrayBuffer[Int]()
      trueUserAssertions.foreach { qa: QualityAssertion => listErrorCodes.append(qa.code) }

      if (AssertionCodes.isGeospatiallyKosher(listErrorCodes.toArray)) {
        properties += (FullRecordMapper.geospatialDecisionColumn -> "true")
      }
      if (AssertionCodes.isTaxonomicallyKosher(listErrorCodes.toArray)) {
        properties += (FullRecordMapper.taxonomicDecisionColumn -> "true")
      }
    }

    //for each qa type get the list of QA's that failed
    val assertionsDeleted = ListBuffer[QualityAssertion]() // stores the assertions that should not be considered for the kosher fields
    for (name <- systemAssertions.keySet) {
      val assertions = systemAssertions.get(name).getOrElse(Array[QualityAssertion]())
      val failedass = new ArrayBuffer[Int]
      assertions.foreach { qa =>
        //only add if it has failed
        if (qa.getQaStatus == 0) {
          //check to see if a user assertion counteracts this code
          if (!doesListContainCode(falseUserAssertions, qa.code))
            failedass.add(qa.code)
          else
            assertionsDeleted += qa
        }
      }

      //add the "true" user assertions to the arrays
      //filter the list based on the name of the phase
      val ua2Add = trueUserAssertions.filter { a =>
        name match {
          case "loc" => a.code >= AssertionCodes.geospatialBounds._1 && a.code < AssertionCodes.geospatialBounds._2
          case "class" => a.code >= AssertionCodes.taxonomicBounds._1 && a.code < AssertionCodes.taxonomicBounds._2
          case "event" => a.code >= AssertionCodes.temporalBounds._1 && a.code < AssertionCodes.temporalBounds._2
          case _ => false
        }
      }

      ua2Add.foreach { qa => if (!failedass.contains(qa.code))
        failedass.add(qa.code)
      }

      properties += (FullRecordMapper.markAsQualityAssertion(name) -> Json.toJSONWithGeneric(failedass.toList))
      if (!verified) {
        if (name == FullRecordMapper.geospatialQa) {
          properties += (FullRecordMapper.geospatialDecisionColumn -> AssertionCodes.isGeospatiallyKosher(failedass.toArray).toString)
        } else if (name == FullRecordMapper.taxonomicalQa) {
          properties += (FullRecordMapper.taxonomicDecisionColumn -> AssertionCodes.isTaxonomicallyKosher(failedass.toArray).toString)
        }
      }
    }
    properties.toMap
  }

  /**
   * Adds a quality assertion to the row with the supplied UUID.
   *
   * @param qualityAssertion
   */
  def addSystemAssertion(rowKey: String, qualityAssertion: QualityAssertion,replaceExistCode:Boolean=false,checkExisting:Boolean=true) {
    val baseAssertions = if(replaceExistCode) {
      (getSystemAssertions(rowKey).filterNot(_.code == qualityAssertion.code) :+ qualityAssertion)
    } else {
      (getSystemAssertions(rowKey) :+ qualityAssertion)
    }
    val systemAssertions = baseAssertions.groupBy(x => x.code).values.map(_.head).toList
    if (checkExisting) {
      val userAssertions = getUserAssertions(rowKey)
      updateAssertionStatus(rowKey, qualityAssertion, systemAssertions, userAssertions)
    }
    persistenceManager.putList(rowKey, entityName, FullRecordMapper.qualityAssertionColumn, systemAssertions.toList, classOf[QualityAssertion], true, false, false)
  }

  /**
   * Remove system assertion, and update status.
   *
   * @param rowKey
   * @param assertionCode
   */
  def removeSystemAssertion(rowKey: String, assertionCode:ErrorCode){
    val systemAssertions = getSystemAssertions(rowKey)
    val newSystemAssertions = systemAssertions.filter(_.code != assertionCode.code)
    val userAssertions = getUserAssertions(rowKey)
    updateAssertionStatus(rowKey, QualityAssertion(assertionCode), systemAssertions, userAssertions)
    persistenceManager.putList(rowKey, entityName, FullRecordMapper.qualityAssertionColumn, newSystemAssertions.toList, classOf[QualityAssertion], false, true, false)
  }

  /**
   * Set the system systemAssertions for a record, overwriting existing systemAssertions
   *
   * Please NOTE a verified record will still have a list of SystemAssertions that failed. But there will be no corresponding qa codes.
   */
  def updateSystemAssertions(rowKey: String, qualityAssertions: Map[String, Array[QualityAssertion]]) {
    var assertions = new ListBuffer[QualityAssertion]
    qualityAssertions.values.foreach(x => { assertions ++= x })
    persistenceManager.putList(rowKey, entityName, FullRecordMapper.qualityAssertionColumn, assertions.toList, classOf[QualityAssertion], false, true, false)
  }

  /**
   * Retrieve annotations for the supplied UUID.
   */
  def getSystemAssertions(rowKey: String): List[QualityAssertion] = {
    persistenceManager.getList(rowKey, entityName, FullRecordMapper.qualityAssertionColumn, classOf[QualityAssertion])
  }

  def extractQAKeyNum(referenceRowKey: String): Int = {
    if (StringUtils.isNotBlank(referenceRowKey)) {
      val value = referenceRowKey.split('|').last
      return Integer.parseInt(value)
    } else {
      return 0
    }
  }

  implicit object QualityAssertionRowKeyOrdering extends Ordering[QualityAssertion] {
    override def compare(x: QualityAssertion, y: QualityAssertion): Int = extractQAKeyNum(x.referenceRowKey).compareTo(extractQAKeyNum(y.referenceRowKey))
  }


  def getNextVerifiedRecordNumber(userAssertions: List[QualityAssertion]): String = {
    val verifiedAssertions = userAssertions.filter(qa => qa.code == AssertionCodes.VERIFIED.code)

    if (verifiedAssertions.size > 0) {
      return (extractQAKeyNum(verifiedAssertions.max.referenceRowKey) + 1).toString
    } else {
      return "1"
    }
  }

  /**
   * Add a user supplied assertion - updating the status on the record.
   */
  def addUserAssertion(rowKey: String, qualityAssertion: QualityAssertion) {

    val userAssertions = getUserAssertions(rowKey)

    //if its not a verification of an existing assertion, its a new one and hence its unconfirmed.
    if (!AssertionCodes.isVerified(qualityAssertion)) {
      qualityAssertion.qaStatus = AssertionStatus.QA_UNCONFIRMED
    }

    qualityAssertion.referenceRowKey = rowKey

    val qualityAssertionProperties = FullRecordMapper.mapObjectToProperties(qualityAssertion)
    val record = this.getRawProcessedByRowKey(rowKey)

    if (!record.isEmpty) {

      //preserve the raw record
      val qaMap = qualityAssertionProperties ++ Map("snapshot" -> Json.toJSON(record.get))
      persistenceManager.put(rowKey, qaEntityName, qaMap, true, false)
      val systemAssertions = getSystemAssertions(rowKey)
      val userAssertions = getUserAssertions(rowKey)
      updateAssertionStatus(rowKey, qualityAssertion, systemAssertions, userAssertions :+ qualityAssertion)

      //set the last user assertion date
      persistenceManager.put(rowKey, entityName, FullRecordMapper.lastUserAssertionDateColumn, qualityAssertion.created, false, false)

      //when the user assertion is verified need to add extra value
      if (AssertionCodes.isVerified(qualityAssertion)) {
        persistenceManager.put(rowKey, entityName, FullRecordMapper.userVerifiedColumn, "true", false, false)
      }
    }
  }

  /**
    * Retrieve annotations for the supplied UUID.
    */
  def getUserAssertions(rowKey: String): List[QualityAssertion] = {
    val result = persistenceManager.getAllByIndex(rowKey, qaEntityName, "rowkey")
    val userAssertions = new ArrayBuffer[QualityAssertion]
    result.foreach { map =>
        val qa = new QualityAssertion()
        FullRecordMapper.mapPropertiesToObject(qa, map)
        userAssertions += qa
    }
    userAssertions.toList
  }

  /**
    * Retrieves a distinct list of user ids for the assertions
    */
  def getUserIdsForAssertions(rowKey: String): Set[String] = {
    val result = persistenceManager.getAllByIndex(rowKey, qaEntityName, "rowkey")
    val userIds =  new ArrayBuffer[String]
    result.foreach { map =>
      val userId = map.get("userId")
      if (!userId.isEmpty)
        userIds += userId.get
    }
    userIds.toSet
  }

  /**
    * Delete a user supplied assertion
    */
  def deleteUserAssertion(rowKey: String, assertionUuid: String): Boolean = {

    logger.debug("Deleting assertion for : " + rowKey + " with assertion uuid : " + assertionUuid)

    val assertions = getUserAssertions(rowKey)
    if (assertions.isEmpty) {
      logger.warn("Unable to locate in assertions for: " + rowKey)
      false
    } else {
      //get the assertion that is to be deleted
      val deletedAssertion:Option[QualityAssertion] = assertions.find { _.uuid equals assertionUuid }

      if (!deletedAssertion.isEmpty) {

        val toBeDeleted = deletedAssertion.get

        persistenceManager.delete(
          Map(
            "rowkey" -> toBeDeleted.referenceRowKey,
            "userId" -> toBeDeleted.getUserId,
            "code"   -> toBeDeleted.code.toString
          ),
          qaEntityName
        )

        val updateAssertions = getUserAssertions(rowKey)
        val systemAssertions = getSystemAssertions(rowKey)

        updateAssertionStatus(rowKey, toBeDeleted, systemAssertions, updateAssertions)
        true
      } else {
        logger.warn("Unable to find assertion with UUID: " + assertionUuid)
        false
      }
    }
  }

  private def getListOfCodes(rowKey: String, phase: String): List[Int] = {
    persistenceManager.getList(rowKey, entityName, FullRecordMapper.markAsQualityAssertion(phase), classOf[Int])
  }

  /**
    * getCombinedUserStatus is used to get User Assertion Status based on the following rules.
    * It also returns an array of open assertions.
    *
    * There are 5 states for User Assertion Status:
    * NONE: If no user assertion record exist
    * UNCONFIRMED: If there is a user assertion but has not been verified by Collection Admin
    * OPEN ISSUE: Collection Admin verifies the record and flags the user assertion as Open issue
    * VERIFIED: Collection Admin verifies the record and flags the user assertion as Verified
    * CORRECTED: Collection Admin verifies the record and flags the user assertion as Corrected
    *
    * If Collection Admin verifies the record, currentAssertion will have
    * code: 50000 (AssertionCodes.VERIFIED.code),
    * qaStatus: AssertionStatus.QA_OPEN_ISSUE, AssertionStatus.QA_VERIFIED, AssertionStatus:QA_CORRECTED
    */
  private def getCombinedUserStatus(bVerified: Boolean, userAssertions: List[QualityAssertion]): (Int, ArrayBuffer[QualityAssertion]) = {

    // Filter off only verified records
    val verifiedAssertions = userAssertions.filter(qa => qa.code == AssertionCodes.VERIFIED.code)

    // Filter off only user assertions type
    val assertions = userAssertions.filter(qa => qa.code != AssertionCodes.VERIFIED.code && AssertionStatus.isUserAssertionType(qa.qaStatus))

    // Sort the verified list according to relatedUuid and order of reference rowKey.
    // RowKey for verified records consist of rowKey|userId|code|recNum where recNum increments everytime a verified record is added
    val sortedList = verifiedAssertions.sortWith(QualityAssertion.compareByReferenceRowKeyDesc).sortBy(_.relatedUuid)

    var latestVerifiedList = new ArrayBuffer[QualityAssertion]()

    // Only extract latest verified assertion in latestVerifiedList
    sortedList.foreach { qa: QualityAssertion =>
      if (!latestVerifiedList.exists(p => p.relatedUuid equals (qa.relatedUuid)))
        latestVerifiedList.append(qa)
    }

    // Get assertions that have not been verified to the assertion list
    var combinedUserAssertions = new ArrayBuffer[QualityAssertion]()
    assertions.foreach { qa: QualityAssertion =>
      if (!latestVerifiedList.exists(p => p.relatedUuid equals (qa.uuid)))
        combinedUserAssertions.append(qa)
    }

    // Default user assertion to none. This will be overwritten later if user assertion is found
    var userAssertionStatus = AssertionStatus.QA_NONE

    // However if it's verified assertion, default to latest verified qa Status, which could be Verified, Corrected or Open Issue
    // Use maxBy referenceRowKey rather than currentAssertion in case currentAssertion is the record that is to be deleted
    //    if (AssertionCodes.isVerified(currentAssertion)) {
    if (bVerified) {
      if (!latestVerifiedList.isEmpty) {
        userAssertionStatus = latestVerifiedList.maxBy(_.referenceRowKey).qaStatus
      } else {
        // assuming that there no more verified records but there are still user assertion records
        userAssertionStatus = AssertionStatus.QA_UNCONFIRMED
      }
      //    userAssertionStatus = currentAssertion.qaStatus
    } else {
      // if it is not verified and it is a user assertion that is deleted we shouldn't ignore the latest status in verified list
      if (!latestVerifiedList.isEmpty) {
        userAssertionStatus = latestVerifiedList.maxBy(_.referenceRowKey).qaStatus
      }
    }

    // still have user assertions that have not been verified.
    if (combinedUserAssertions.size > 0) {
      userAssertionStatus = AssertionStatus.QA_UNCONFIRMED
    } else {
      // If all user assertions have been verified, check to see if any verification is set to Open Issue
      latestVerifiedList.foreach { qa: QualityAssertion =>
        if (qa.qaStatus == AssertionStatus.QA_OPEN_ISSUE) {
          combinedUserAssertions = combinedUserAssertions ++ (assertions.filter(a => a.uuid == qa.relatedUuid))
          userAssertionStatus = AssertionStatus.QA_OPEN_ISSUE
        }
      }
    }

    logger.debug("Overall assertion Status: " + userAssertionStatus)

    (userAssertionStatus, combinedUserAssertions)
  }

  /**
    * Update the assertion status using system and user assertions.
    */
  def updateAssertionStatus(rowKey: String, assertion: QualityAssertion, systemAssertions: List[QualityAssertion], userAssertions: List[QualityAssertion]) {

    logger.debug("Updating the assertion status for : " + rowKey)

    val bVerified = AssertionCodes.isVerified(assertion)
    val (userAssertionStatus, remainingAssertions) = getCombinedUserStatus(bVerified, userAssertions)

    // default to the assertion which is to be evaluated
    var actualAssertion = assertion
    var bVerifiedOpen = true

    // if this is verified assertion, replace with the actual user assertion which is still open
    if (AssertionCodes.isVerified(assertion)) {
      var bStillOpen = false
      remainingAssertions.foreach { qa =>
        if (qa.uuid == assertion.relatedUuid) {
          actualAssertion = qa
          bStillOpen = true
        }
      }

      if (!bStillOpen) {
        userAssertions.foreach { qa =>
            if (qa.uuid == assertion.relatedUuid) {
              actualAssertion = qa
              bVerifiedOpen = false
            }
        }
      }
    }

    //get the phase based on the error type
    val phase = Processors.getProcessorForError(actualAssertion.code)
    logger.debug("Phase " + phase)

    //get existing values for the phase
    var listErrorCodes: Set[Int] = getListOfCodes(rowKey, phase).toSet
    logger.debug("Original: " + listErrorCodes)

    val assertionName = actualAssertion.name
    val assertions = (remainingAssertions).filter { _.name equals assertionName }

    //if the a user assertion has been set for the supplied QA we will set the status bases on user assertions
    if (!assertions.isEmpty) {

      // the assertion has been verified Corrected or verified Verified
      if (!bVerifiedOpen) {
        //need to remove this assertion from the error codes if it exists
        listErrorCodes = listErrorCodes - actualAssertion.code
      } else {
        //at least one user has flagged this assertion so we need to add it
        listErrorCodes = listErrorCodes + actualAssertion.code
      }
    } else if (!systemAssertions.isEmpty) {
      //check to see if a system assertion exists
      val matchingAssertion = systemAssertions.find {
        _.name equals assertionName
      }
      if (!matchingAssertion.isEmpty) {
        //this assertion has been set by the system
        val sysassertion = matchingAssertion.get
        listErrorCodes = listErrorCodes + sysassertion.code
      } else {
        //code needs to be removed
        listErrorCodes = listErrorCodes - actualAssertion.code
      }
    } else {
      //there are no matching assertions in user or system thus remove this error code
      listErrorCodes = listErrorCodes - actualAssertion.code
    }

    logger.debug("Final " + listErrorCodes)

    //update the list
    persistenceManager.put(rowKey, entityName, FullRecordMapper.userQualityAssertionColumn, Json.toJSON(remainingAssertions.toList), false, false)
    persistenceManager.putList(rowKey, entityName, FullRecordMapper.markAsQualityAssertion(phase), listErrorCodes.toList, classOf[Int], false, true, false)

    //set the overall decision if necessary
    var properties = scala.collection.mutable.Map[String, String]()
    //need to update the user assertion flag in the occurrence record

    properties += (FullRecordMapper.userAssertionStatusColumn -> userAssertionStatus.toString)
    //  properties += (FullRecordMapper.userQualityAssertionColumn -> remainingAssertions.toList.toString())

    //  properties += (FullRecordMapper.userQualityAssertionColumn -> (userAssertions.size>0).toString)
    if (AssertionCodes.isVerified(assertion)) {

      if (AssertionCodes.isGeospatiallyKosher(listErrorCodes.toArray)) {
        properties += (FullRecordMapper.geospatialDecisionColumn -> "true")
      }
      if (AssertionCodes.isTaxonomicallyKosher(listErrorCodes.toArray)) {
        properties += (FullRecordMapper.taxonomicDecisionColumn -> "true")
      }
    } else if (phase == FullRecordMapper.geospatialQa) {
      properties += (FullRecordMapper.geospatialDecisionColumn -> AssertionCodes.isGeospatiallyKosher(listErrorCodes.toArray).toString)
    } else if (phase == FullRecordMapper.taxonomicalQa) {
      properties += (FullRecordMapper.taxonomicDecisionColumn -> AssertionCodes.isTaxonomicallyKosher(listErrorCodes.toArray).toString)
    }
    if (!properties.isEmpty) {
      logger.debug("Updating the assertion status for : " + rowKey + properties)
      persistenceManager.put(rowKey, entityName, properties.toMap, false, false)
    }
  }

  /**
    * Set this record to deleted.
    */
  def setDeleted(rowKey: String, del: Boolean, dateTime: Option[String] = None) = {
    if (dateTime.isDefined) {
      val values = Map(FullRecordMapper.deletedColumn -> del.toString, FullRecordMapper.dateDeletedColumn -> dateTime.get)
      persistenceManager.put(rowKey, entityName, values, false, false)
    } else {
      persistenceManager.put(rowKey, entityName, FullRecordMapper.deletedColumn, del.toString, false, false)
    }
    //remove the datedeleted column if the records becomes undeleted...
    if (!del)
      persistenceManager.deleteColumns(rowKey, entityName, FullRecordMapper.dateDeletedColumn)
  }

  /**
    * Returns the rowKey based on the supplied uuid
    */
  def getRowKeyFromUuid(uuid: String): Option[String] = {
    def rk = getRowKeyFromUuidDB(uuid)

    if (rk.isDefined) {
      rk
    } else {
      //work around so that index is searched if it can't be found in the cassandra secondary index.
      getRowKeyFromUuidIndex(uuid)
    }
  }

  def getRowKeyFromUuidDB(uuid: String): Option[String] = persistenceManager.getByIndex(uuid, entityName, UUID, ROW_KEY)

  def getRowKeyFromUuidIndex(uuid: String): Option[String] = {
    if (uuid.startsWith("dr")) {
      Some(uuid)
    } else {
      val list = Config.indexDAO.getRowKeysForQuery("id:" + uuid, 1)
      if(list.isDefined){
        list.get.headOption
      } else {
        None
      }
    }
  }

  /**
    * Should be possible to factor this out
    */
  def reIndex(rowKey: String) {
    logger.debug("Reindexing rowkey: " + rowKey)
    //val map = persistenceManager.getByIndex(uuid, entityName, "uuid")
    val map = persistenceManager.get(rowKey, entityName)
    //index from the map - this should be more efficient
    if (map.isEmpty) {
      logger.debug("Unable to reindex : " + rowKey)
    } else {
      val csvFileWriter = if (Config.exportIndexAsCsvPath.length > 0) {
        indexDAO.getCsvWriter()
      } else {
        null
      }
      val csvFileWriterSensitive = if (Config.exportIndexAsCsvPathSensitive.length > 0) {
        indexDAO.getCsvWriter(true)
      } else {
        null
      }
      indexDAO.indexFromMap(rowKey, map.get, batch = false, csvFileWriter = csvFileWriter, csvFileWriterSensitive = csvFileWriterSensitive)
      if (csvFileWriter != null) {
        csvFileWriter.flush();
        csvFileWriter.close()
      }
      if (csvFileWriterSensitive != null) {
        csvFileWriterSensitive.flush();
        csvFileWriterSensitive.close()
      }
    }
  }

  /**
   * Delete the record for the supplied UUID.
   *
   * @param rowKey
   * @param removeFromIndex
   * @param logDeleted
   * @return
   */
  def delete(rowKey: String, removeFromIndex:Boolean=true, logDeleted:Boolean=false) : Boolean = {

    if (rowKey != "") {
      if (logDeleted) {
        val map = persistenceManager.get(rowKey, entityName)
        if (map !=null && !map.isEmpty){
          val stringValue = Json.toJSON(map.get)
          //log the deleted record to history
          //get the map version of the record
          val deletedTimestamp = org.apache.commons.lang.time.DateFormatUtils.format(new java.util.Date, "yyyy-MM-dd HH:mm:ss")
          val values = Map("id" -> rowKey, "value" -> stringValue)
          persistenceManager.put(deletedTimestamp, "dellog", values, true, false)
        }
      }
      //delete from the data store
      persistenceManager.delete(rowKey, entityName)
    }

    //delete from the index
    if (removeFromIndex) {
      indexDAO.removeFromIndex("id", rowKey)
    }
    true
  }

  def isSensitive(fr: FullRecord): Boolean = {
    StringUtils.isNotBlank(fr.occurrence.getDataGeneralizations)
  }

}
