package au.org.ala.biocache

import java.io.{File, OutputStream}
import java.net.URL
import java.util.Date

import au.org.ala.biocache.dao.{OccurrenceDAO, OutlierStatsDAO}
import au.org.ala.biocache.index.{IndexFields, IndexRecords}
import au.org.ala.biocache.load._
import au.org.ala.biocache.model._
import au.org.ala.biocache.outliers.{JackKnifeStats, RecordJackKnifeStats, SampledRecord}
import au.org.ala.biocache.parser.ProcessedValue
import au.org.ala.biocache.processor.RecordProcessor
import au.org.ala.biocache.qa.ValidationRuleRunner
import au.org.ala.biocache.tool._
import au.org.ala.biocache.util._
import au.org.ala.biocache.vocab.{ErrorCode, SpeciesGroup, _}
import au.org.ala.layers.dao.IntersectCallback
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

/**
  * This is the interface to use for java applications or any application using this as a library.
  *
  * This will allow apps to:
  * <ul>
  * <li> Retrieve single record, three versions </li>
  * <li> Page over records </li>
  * <li> Add user supplied or system systemAssertions for records </li>
  * <li> Add user supplied corrections to records </li>
  * <li> Record downloads </li>
  * <li> Add records in a temporary space </li>
  * </ul>
  *
  * ...and lots more.
  */
object Store {

  val logger = LoggerFactory.getLogger("Store")

  private val occurrenceDAO = Config.getInstance(classOf[OccurrenceDAO]).asInstanceOf[OccurrenceDAO]
  private val outlierStatsDAO = Config.getInstance(classOf[OutlierStatsDAO]).asInstanceOf[OutlierStatsDAO]
  private val deletedRecordDAO = Config.deletedRecordDAO
  private val duplicateDAO = Config.duplicateDAO
  private val validationRuleDAO = Config.validationRuleDAO
  private var readOnly = false

  import BiocacheConversions._

  import JavaConversions._
  import scala.collection.JavaConverters._

  /**
    * A java API friendly version of the getByUuid that does not require knowledge of a scala type.
    */
  def getByUuid(uuid: java.lang.String, version: Version): FullRecord =
    occurrenceDAO.getByRowKey(uuid, version).getOrElse(null)

  def getSensitiveByUuid(uuid:java.lang.String, version:Version):FullRecord =
    occurrenceDAO.getByRowKey(uuid, version, true).getOrElse(null)

  /**
   * A java API friendly version of the getByUuid that doesnt require knowledge of a scala type.
   */
  def getByUuid(uuid: java.lang.String): FullRecord = occurrenceDAO.getByRowKey(uuid, Raw).getOrElse(null)

  /**
   * Retrieve all versions of the record with the supplied UUID.
   */
  def getAllVersionsByUuid(uuid: java.lang.String, includeSensitive:java.lang.Boolean) : Array[FullRecord] =
    occurrenceDAO.getAllVersionsByRowKey(uuid, includeSensitive).getOrElse(null)

  /**
   * Get the raw processed comparison based on the uuid for the occurrence.
   */
  def getComparisonByUuid(uuid: java.lang.String) : java.util.Map[String,java.util.List[ProcessedValue]] =
    getComparison(occurrenceDAO.getAllVersionsByRowKey(uuid).getOrElse(null))

  /**
    * Generate a comparison of the supplied records.
    *
    * @param recordVersions
    * @return
    */
  private def getComparison(recordVersions: Array[FullRecord]): java.util.Map[String, java.util.List[ProcessedValue]] = {
    if (recordVersions != null && recordVersions.length > 1) {
      val map = new java.util.HashMap[String, java.util.List[ProcessedValue]]

      val raw = recordVersions(0)
      val processed = recordVersions(1)

      val rawAndProcessed = raw.objectArray zip processed.objectArray

      rawAndProcessed.foreach(rawAndProcessed => {

        val (rawPoso, procPoso) = rawAndProcessed
        val listBuff = new java.util.LinkedList[ProcessedValue]

        rawPoso.getPropertyNames.foreach { name =>
          if (!Config.sensitiveFields.contains(name)) {
            val rawValue = rawPoso.getProperty(name)
            val procValue = procPoso.getProperty(name)
            if (!rawValue.isEmpty || !procValue.isEmpty) {
              val term = ProcessedValue(name, rawValue.getOrElse(""), procValue.getOrElse(""))
              listBuff.add(term)
            }
          }
        }

        val name = rawPoso.getClass().getName().substring(rawPoso.getClass().getName().lastIndexOf(".") + 1)
        map.put(name, listBuff)
      })

      map
    } else {
      new java.util.HashMap[String, java.util.List[ProcessedValue]]()
    }
  }

  /**
    * Iterate over records, passing the records to the supplied consumer.
    */
  def pageOverAll(version: Version, consumer: OccurrenceConsumer, dataResourceUid: String, pageSize: Int) {
    occurrenceDAO.pageOverAll(version, fullRecord => consumer.consume(fullRecord.get), dataResourceUid, pageSize)
  }

  /**
    * Page over all versions of the record, handing off to the OccurrenceVersionConsumer.
    */
  def pageOverAllVersions(consumer: OccurrenceVersionConsumer, dataResourceUid: String, pageSize: Int) {
    occurrenceDAO.pageOverAllVersions(fullRecordVersion => {
      if (!fullRecordVersion.isEmpty) {
        consumer.consume(fullRecordVersion.get)
      } else {
        true
      }
    }, dataResourceUid, pageSize)
  }

  def rowKeyFile(dataResourceIdentifer: String): File = {
    new File(Config.tmpWorkDir + "/row_key_" + dataResourceIdentifer + ".csv")
  }

  /**
    * Load the record, download any media associated with the record but avoid processing the record.
    */
  def loadRecordOnly(dataResourceUid: String, fr: FullRecord, identifyingTerms: java.util.List[String]) {
    fr.lastModifiedTime = new Date()
    (new SimpleLoader).load(dataResourceUid, fr, identifyingTerms.toList, true, true, false)
  }

  /**
    * Adds or updates a raw full record with values that are in the FullRecord
    * relies on a rowKey being set. This method only loads the record.
    *
    * Record is indexed if should index is true
    */
  def loadRecord(dataResourceIdentifer:String, properties:java.util.Map[String,String], shouldIndex:Boolean){
    (new RecordProcessor).addRecord(dataResourceIdentifer, properties.toMap[String,String])
  }

  /**
    * Load the record, download any media associated with the record.
    */
  def loadRecord(dataResourceUid: String, fr: FullRecord, identifyingTerms: java.util.List[String], shouldIndex: Boolean = true) {
    fr.lastModifiedTime = new Date()
    (new SimpleLoader).load(dataResourceUid, fr, identifyingTerms.toList, true, true, false)
    val processor = new RecordProcessor
    processor.processRecordAndUpdate(fr)
    if (shouldIndex) {
      occurrenceDAO.reIndex(fr.rowKey)
    }
  }

  /**
    * Loads a batch of records based on being supplied in a list of maps.
    *
    * It relies on identifyFields supplying a list dwc terms that make up the unique identifier for the data resource
    */
  def loadRecords(dataResourceUid: String, recordsProperties: java.util.List[java.util.Map[String, String]],
                  identifyFields: java.util.List[String], shouldIndex: Boolean = true) {
    val loader = new MapDataLoader
    val rowKeys = loader.load(dataResourceUid, recordsProperties.toList, identifyFields.toList)
    if (!rowKeys.isEmpty) {
      ProcessRecords.processRowKeys(rowKeys)
      if (shouldIndex) {
        IndexRecords.indexList(rowKeys)
      }
    }
  }

  /**
    * Adds or updates a raw full record with values that are in the FullRecord
    *
    * Record is processed and indexed if should index is true
    */
  def upsertRecord(dataResourceUid: String,
                   properties: java.util.Map[String, String],
                   multimediaProperties: java.util.List[java.util.Map[String, String]],
                   shouldIndex: Boolean): FullRecord = {

    import JavaConversions._
    val loader = new SimpleLoader

    //retrieve details
    loader.retrieveConnectionParameters(dataResourceUid) match {
      case None => throw new Exception("Unable to retrieve connection information for data resource UID: " + dataResourceUid)
      case Some(dataResourceConfig) =>

        //create a record
        val record = FullRecordMapper.createFullRecord("", properties, Versions.RAW)

        //map multimedia
        val multimedia = multimediaProperties.map { props =>
          Multimedia.create(new URL(props.getOrElse("identifier", "")), "", props.asScala.toMap)
        }

        //load the record
        (new SimpleLoader()).load(dataResourceUid, record, dataResourceConfig.uniqueTerms, multimedia)

        //process record
        val processor = new RecordProcessor
        processor.processRecordAndUpdate(record)

        //index
        if (shouldIndex) {
          occurrenceDAO.reIndex(record.rowKey)
        }
        record
    }
  }

  /**
    * Deletes the records for the supplied uuid from the index and data store
    */
  def deleteRecordBy(uuid: String) = if (uuid != null) occurrenceDAO.delete(uuid)

  /**
    * Deletes the supplied list of row keys from the index and data store
    */
  def deleteRecords(rowKeys: java.util.List[String]) {
    val deletor = new ListDelete(rowKeys.toList)
    deletor.deleteFromPersistent
    deletor.deleteFromIndex
  }

  /**
   * Delete records matching the supplied query or data resource
   *
   * @param dataResource
   * @param query
   * @param fromPersistent
   * @param fromIndex
   */
  def deleteRecords(dataResource:java.lang.String, query:java.lang.String, fromPersistent:Boolean, fromIndex:Boolean) : Unit = {
    val deletor:RecordDeletor = if(dataResource != null) {
      new QueryDelete("data_resource_uid:" + dataResource)
    } else {
      new QueryDelete(query)
    }
    logger.debug("Delete from storage using the query: " + query)
    if (fromPersistent) {
      deletor.deleteFromPersistent
    }
    logger.debug("Delete from index")
    if (fromIndex) {
      deletor.deleteFromIndex
    }
    if (fromPersistent && fromIndex) {
      logger.debug("Delete row_key file")
      FileUtils.deleteQuietly(rowKeyFile(dataResource))
    }
    logger.debug("Delete complete.")
  }

  /**
    * Get system assertions that have failed.
    *
    * @param uuid
    * @return
    */
  def getSystemAssertions(uuid: java.lang.String): java.util.List[QualityAssertion] = {
    val rowKey = occurrenceDAO.getRowKeyFromUuid(uuid).getOrElse(uuid)
    occurrenceDAO.getSystemAssertions(rowKey).filter(_.qaStatus == 0).asJava
  }

  /**
    * Retrieve the  system assertions for a record.
    *
    * A user can supply either a uuid
    */
  def getAllSystemAssertions(uuid: java.lang.String): java.util.Map[String, java.util.List[QualityAssertion]] = {
    //system assertions are handled using row keys - this is unlike user assertions.
    val rowKey = occurrenceDAO.getRowKeyFromUuid(uuid).getOrElse(uuid)
    val list = occurrenceDAO.getSystemAssertions(rowKey)
    val unchecked = AssertionCodes.getMissingCodes((list.map(it => AssertionCodes.getByCode(it.code).getOrElse(null))).toSet)

    ((list ++ unchecked.map(it => QualityAssertion(it, AssertionStatus.UNCHECKED))).groupBy {
      case i if (i.qaStatus == 0) => {
        AssertionCodes.getByCode(i.code).getOrElse(AssertionCodes.GEOSPATIAL_ISSUE).category match {
          case ErrorCodeCategory.Error => "failed"
          case code: String => code.toString.toLowerCase
        }
      }
      case i if i.qaStatus == AssertionStatus.PASSED => "passed"
      case _ => "unchecked"
    }).mapValues(_.asJava).asJava
  }

  /**
    * Retrieve the user supplied systemAssertions.
    */
  def getUserAssertion(uuid: java.lang.String, assertionUuid: java.lang.String): QualityAssertion = {
    val rowKey = occurrenceDAO.getRowKeyFromUuid(uuid).getOrElse(uuid)
    occurrenceDAO.getUserAssertions(rowKey).find { ass =>
      ass.uuid == assertionUuid
    }.getOrElse(null)
  }

  /**
    * Retrieve the user supplied systemAssertions.
    */
  def getUserAssertions(uuid: java.lang.String): java.util.List[QualityAssertion] = {
    val rowKey = occurrenceDAO.getRowKeyFromUuid(uuid).getOrElse(uuid)
    occurrenceDAO.getUserAssertions(rowKey).asJava
  }

  /**
    * Add a user assertion
    *
    * Requires a re-index
    */
  def addUserAssertion(uuid: java.lang.String, qualityAssertion: QualityAssertion) {
    if (!readOnly) {
      val rowKey = occurrenceDAO.getRowKeyFromUuid(uuid).getOrElse(uuid)
      occurrenceDAO.addUserAssertion(rowKey, qualityAssertion)
      occurrenceDAO.reIndex(rowKey)
    } else {
      throw new Exception("In read only model. Please try again later")
    }
  }

  /**
    * Adds the supplied list of qa's to the data store and reindexes to allow changes to be available in the search
    *
    * @param assertionMap
    */
  def addUserAssertions(assertionMap: java.util.Map[String, QualityAssertion]) {
    if (!readOnly) {
      val arrayBuffer = new ArrayBuffer[String]()
      var count = 0
      assertionMap.foreach({
        case (uuid, qa) => {
          count += 1
          //apply the assertion and add to the reindex list
          val rowKey = occurrenceDAO.getRowKeyFromUuid(uuid).getOrElse(uuid)
          occurrenceDAO.addUserAssertion(rowKey, qa)
          arrayBuffer += rowKey
        }
      })
      // now reindex
      IndexRecords.indexList(arrayBuffer.toList)
      logger.debug("Added " + count + " user assertions in bulk")
    } else {
      throw new Exception("In read only model. Please try again later")
    }
  }

  def addValidationRule(validationRule: ValidationRule) = validationRuleDAO.upsert(validationRule)

  def getValidationRule(uuid: java.lang.String): ValidationRule = validationRuleDAO.get(uuid).getOrElse(null)

  def getValidationRules(ids: Array[String]): Array[ValidationRule] = validationRuleDAO.get(ids.toList).toArray[ValidationRule]

  def getValidationRules: Array[ValidationRule] = validationRuleDAO.list.toArray[ValidationRule]

  /**
    * Applies the supplied query assertion to the records.
    */
  def applyValidationRule(uuid: java.lang.String) = new ValidationRuleRunner().applySingle(uuid)

  def deleteValidationRule(id: java.lang.String, date: java.util.Date) = validationRuleDAO.delete(id, date)

  /**
    * Delete an assertion
    *
    * Requires a re-index
    */
  def deleteUserAssertion(uuid: java.lang.String, assertionUuid: java.lang.String) {
    if (!readOnly) {
      val rowKey = occurrenceDAO.getRowKeyFromUuid(uuid).getOrElse(uuid);
      occurrenceDAO.deleteUserAssertion(rowKey, assertionUuid)
      occurrenceDAO.reIndex(rowKey)
    } else {
      throw new Exception("In read only model. Please try again later")
    }
  }

  /**
    * Puts biocache store into readonly model.
    * Useful when we don't want services to update the index.
    * This is generally when a optimise is occurring
    */
  def setReadOnly(ro: Boolean): Unit = readOnly = ro

  def isReadOnly = readOnly

  def optimiseIndex(): String = {
    val start = System.currentTimeMillis
    readOnly = true
    try {
      val indexString = Config.indexDAO.optimise
      val finished = System.currentTimeMillis
      readOnly = false
      "Optimised in " + (finished - start).toFloat / 60000f + " minutes.\n" + indexString
    } catch {
      case e: Exception => {
        //report error message and take out of readOnly
        readOnly = false
        e.getMessage
      }
    }
  }

  /**
    * Reopens the current index to account for external index changes
    */
  def reopenIndex = Config.indexDAO.reload

  /**
    * Indexes a dataResource from a specific date
    */
  def reindex(dataResource: java.lang.String, startDate: java.lang.String) {
    throw new RuntimeException("Not supported")
    //    if(dataResource != null && startDate != null) {
    //      IndexRecords.index(Some(dataResource), false, false, startDate = Some(startDate))
    //    } else {
    //      throw new Exception("Must supply data resource and start date")
    //    }
  }

  /**
    * Indexes a dataResource from a specific date
    *
    * @param dataResource the resource to index
    */
  def index(dataResource: java.lang.String) =
    throw new RuntimeException("Not supported")

  /**
    * Index a resource, indexing custom fields
    *
    * @param dataResource                      the resource to index
    * @param customIndexFields                 the additional fields to index on top of the default set of fields
    * @param userProvidedTypeCustomIndexFields the additional fields which type is defined by the user
    * @param callback                          a callback used for monitoring the process
    */
  def index(dataResource: java.lang.String, customIndexFields: Array[String], userProvidedTypeCustomIndexFields: Array[String], callback: ObserverCallback = null) = {
    logger.info("Indexing data resource " + dataResource)

    //TODO: update for cassandra3/solr6. This is used by SANDBOX.
    IndexRecords.index(
      Some(dataResource),
      miscIndexProperties = customIndexFields.toSeq,
      userProvidedTypeMiscIndexProperties = userProvidedTypeCustomIndexFields.toSeq,
      callback = callback)
    logger.info("Finished indexing data resource " + dataResource)
    logger.info("Storing custom index fields to the database....")
    storeCustomIndexFields(dataResource, customIndexFields)
    logger.info("Storing custom index fields to the database....done")
  }

  /**
    * Run the sampling for this dataset
    *
    * @param dataResourceUid
    */
  def sample(dataResourceUid: java.lang.String) = Sampling.sampleDataResource(dataResourceUid)

  /**
    * Run the sampling for this dataset
    *
    * @param dataResourceUid
    */
  def sample(dataResourceUid: java.lang.String, callback: IntersectCallback) =
    Sampling.sampleDataResource(dataResourceUid, callback)

  /**
    * Process records for the supplied resource
    *
    * @param dataResourceUid
    * @param threads
    */
  def process(dataResourceUid: java.lang.String, threads: Int = 1, callback: ObserverCallback = null) = {
    //much faster when there is a rowkey file for this dataResourceUid
    val keyFile = rowKeyFile(dataResourceUid)
    val file = if (keyFile.exists()) {
      keyFile
    } else {
      null
    }
    ProcessRecords.processRecords(dataResourceUid, threads)
  }

  /**
    * Writes the select records to the stream. Optionally including the sensitive values.
    */
  def writeToStream(outputStream: OutputStream, fieldDelimiter: java.lang.String,
                    recordDelimiter: java.lang.String, keys: Array[String], fields: Array[java.lang.String], qaFields: Array[java.lang.String], includeSensitive: Boolean) {
    occurrenceDAO.writeToStream(outputStream, fieldDelimiter, recordDelimiter, keys, fields, qaFields, includeSensitive)
  }

  /**
    * Writes the select records to the stream.
    */
  def writeToStream(outputStream: OutputStream, fieldDelimiter: java.lang.String,
                    recordDelimiter: java.lang.String, keys: Array[String], fields: Array[java.lang.String], qaFields: Array[java.lang.String]) {
    writeToStream(outputStream, fieldDelimiter, recordDelimiter, keys, fields, qaFields, false)
  }

  def writeToWriter(writer: RecordWriter, keys: Array[String], fields: Array[java.lang.String], qaFields: Array[java.lang.String], includeSensitive: Boolean) {
    occurrenceDAO.writeToRecordWriter(writer, keys, fields, qaFields, includeSensitive, false, null)
  }

  def writeToWriter(writer: RecordWriter, keys: Array[String], fields: Array[java.lang.String], qaFields: Array[java.lang.String]) {
    writeToWriter(writer, keys, fields, qaFields, false, false, null)
  }

  def writeToWriter(writer: RecordWriter, keys: Array[String], fields: Array[java.lang.String], qaFields: Array[java.lang.String], includeSensitive: Boolean, includeMisc: Boolean, miscFields: Array[java.lang.String]): Array[java.lang.String] = {
    occurrenceDAO.writeToRecordWriter(writer, keys, fields, qaFields, includeSensitive, includeMisc, miscFields)
  }

  def writeToWriter(writer: RecordWriter, keys: Array[String], fields: Array[java.lang.String], qaFields: Array[java.lang.String], includeSensitive: Boolean, includeMisc: Boolean, miscFields: Array[java.lang.String], dataToInsert: java.util.Map[String, Array[String]]): Array[java.lang.String] = {
    occurrenceDAO.writeToRecordWriter(writer, keys, fields, qaFields, includeSensitive, includeMisc, miscFields, dataToInsert)
  }

  /**
    * Retrieve the assertion codes
    */
  def retrieveAssertionCodes: Array[ErrorCode] = AssertionCodes.all.toArray

  /**
    * Retrieve the geospatial codes.
    */
  def retrieveGeospatialCodes: Array[ErrorCode] = AssertionCodes.geospatialCodes.toArray

  /**
    * Retrieve the taxonomic codes.
    */
  def retrieveTaxonomicCodes: Array[ErrorCode] = AssertionCodes.taxonomicCodes.toArray

  /**
    * Retrieve temporal codes
    */
  def retrieveTemporalCodes: Array[ErrorCode] = AssertionCodes.temporalCodes.toArray

  /**
    * Retrieve miscellaneous codes
    */
  def retrieveMiscellaneousCodes: Array[ErrorCode] = AssertionCodes.miscellaneousCodes.toArray

  /**
    * A user friendly set of assertion types.
    */
  def retrieveUserAssertionCodes: Array[ErrorCode] = AssertionCodes.userAssertionCodes.toArray

  /**
    * Retrieve an error code by code.
    */
  def getByCode(codeAsString: String): ErrorCode = {
    val code = codeAsString.toInt
    AssertionCodes.all.find(errorCode => errorCode.code == code).getOrElse(null)
  }

  /**
    * Retrieve the configuration used for species subgroups
    *
    * @return
    */
  def retrieveSubgroupsConfig: String = SpeciesGroups.getSubgroupsConfig

  /**
    * Retrieve the list of species groups
    */
  def retrieveSpeciesGroups: java.util.List[SpeciesGroup] = SpeciesGroups.groups.asJava

  /**
    * Retrieve the list of species groups
    */
  def retrieveSpeciesSubgroups: java.util.List[SpeciesGroup] = SpeciesGroups.subgroups.asJava

  /**
    * Retrieves a map of index fields to storage fields
    */
  def getIndexFieldMap: java.util.Map[String, String] = IndexFields.indexFieldMap

  def getStorageFieldMap: java.util.Map[String, String] = IndexFields.storeFieldMap

  /**
    * Returns the biocache id for the supplied layername
    */
  def getLayerId(name: String): String = if (name != null) {
    Layers.nameToIdMap.getOrElse(name.toLowerCase, null)
  } else {
    null
  }

  /**
    * Ingest the supplied data resources when null is provided all available
    * data resources will be ingested.
    *
    * @param dataResources
    * @param numThreads
    */
  def ingest(dataResources: Array[String], numThreads: Int = 4) {
    //when null is provided as a argument we need to get a list of the available data resources
    val l = new Loader
    if (dataResources != null) {
      dataResources.foreach { dr => IngestTool.ingestResource(dr, numThreads) }
    } else {
      l.resourceList.foreach { resource =>
        val uid = resource.getOrElse("uid", "")
        val name = resource.getOrElse("name", "")
        logger.info(s"Ingesting resource $name, uid: $uid")
        IngestTool.ingestResource(uid, numThreads)
      }
    }
  }

  /**
    * Returns the spatial name for the supplied biocache layer id
    */
  def getLayerName(id: String): String = if (id != null) {
    Layers.idToNameMap.getOrElse(id, null)
  } else {
    null
  }

  def getSoundFormats(filePath: String): java.util.Map[String, String] = Config.mediaStore.getSoundFormats(filePath)

  /**
    * Get jackknife statistics for the supplied species GUID.
    *
    * @param guid
    * @return
    */
  def getJackKnifeStatsFor(guid: String): java.util.Map[String, JackKnifeStats] = outlierStatsDAO.getJackKnifeStatsFor(guid)

  /**
    * Get jackknife record details for the supplied species GUID.
    *
    * @param guid
    * @return
    */
  def getJackKnifeOutliersFor(guid: String): java.util.List[(String, java.util.List[SampledRecord])] = outlierStatsDAO.getJackKnifeOutliersFor(guid)

  /**
    * Retrieve the jack knife details for the supplied UUID.
    *
    * @param uuid
    * @return
    */
  def getJackKnifeRecordDetailsFor(uuid: String): java.util.List[RecordJackKnifeStats] = outlierStatsDAO.getJackKnifeRecordDetailsFor(uuid)

  /**
    * Retrieve the duplicate details for the supplied UUID.
    *
    * @param uuid
    * @return
    */
  def getDuplicateDetails(uuid: String): DuplicateRecordDetails = duplicateDAO.getDuplicateInfo(uuid).getOrElse(new DuplicateRecordDetails())

  /**
    * Returns a list of record uuids that have been deleted since the supplied date inclusive
    */
  def getDeletedRecords(date: java.util.Date): Array[String] =
    deletedRecordDAO.getUuidsForDeletedRecords(org.apache.commons.lang.time.DateFormatUtils.format(date, "yyyy-MM-dd"))

  /**
    * Persist custom index fields.
    *
    * @param tempUid
    * @param customIndexFields
    */
  def storeCustomIndexFields(tempUid: String, customIndexFields: Array[String]) {
    Config.persistenceManager.put(
      tempUid,
      "upload",
      "customIndexFields",
      Json.toJSON(customIndexFields.map(v => if (v.endsWith("_i") || v.endsWith("_d")) v else v + "_s")),
      true,
      false
    )
  }

  /**
    * Retrieve custom index fields.
    *
    * @param tempUid
    * @return
    */
  def retrieveCustomIndexFields(tempUid: String): Array[String] = {
    try {
      val s = Config.persistenceManager.get(tempUid, "upload", "customIndexFields")
      Json.toStringArray(s.getOrElse("[]"))
    } catch {
      case _: Exception => Array()
    }
  }

  /**
    * Persist custom index fields.
    *
    * @param tempUid
    * @param customChartOptions
    */
  def storeCustomChartOptions(tempUid: String, customChartOptions: String): Unit = {
    Config.persistenceManager.put(tempUid, "upload", "customChartOptions", customChartOptions, true, false)
  }

  /**
    * Retrieve custom index fields.
    *
    * @param tempUid
    * @return
    */
  def retrieveCustomChartOptions(tempUid: String): String = {
    try {
      val s = Config.persistenceManager.get(tempUid, "upload", "customChartOptions")
      val json = s.getOrElse("[]")
      json
    } catch {
      case _: Exception => "[]"
    }
  }

  /**
    * Persist custom index fields.
    *
    * @param tempUid
    * @param customChartOptions
    */
  def storeLayerOptions(tempUid: String, customChartOptions: String): String =
    Config.persistenceManager.put(tempUid, "upload", "layerOptions", customChartOptions, true, false)

  /**
    * Retrieve custom index fields.
    *
    * @param tempUid
    * @return
    */
  def retrieveLayerOptions(tempUid: String): String = {
    try {
      val s = Config.persistenceManager.get(tempUid, "upload", "layerOptions")
      val json = s.getOrElse("[]")
      json
    } catch {
      case _: Exception => "[]"
    }
  }
}

/**
  * A trait to implement by java classes to process occurrence records.
  */
trait ObserverCallback {
  def progressMessage(recordCount: Int)
}

/**
  * A trait to implement by java classes to process occurrence records.
  */
trait OccurrenceConsumer {
  /** Consume the supplied record */
  def consume(record: FullRecord): Boolean
}

/**
  * A trait to implement by java classes to process occurrence records.
  */
trait OccurrenceVersionConsumer {
  /** Passes an array of versions. Raw, Process and consensus versions */
  def consume(record: Array[FullRecord]): Boolean
}

/**
  * A trait to be implemented by java classes to write records.
  */
trait RecordWriter {
  /** Any resource allocation operations that are cleaned up in the 
   *  finalise method must be performed during this initialise method. 
   */
  def initialise

  /** Writes the supplied record. */
  def write(record: Array[String])

  /** Performs all the finishing tasks in writing the download file. */
  def finalise

  /** Returns true if this record writer has been finalised */
  def finalised(): Boolean
}

