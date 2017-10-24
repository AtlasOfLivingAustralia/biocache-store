package au.org.ala.biocache.index

import java.io.File

import au.org.ala.biocache._
import au.org.ala.biocache.caches.TaxonProfileDAO
import au.org.ala.biocache.index.lucene.{DocBuilder, LuceneIndexing}
import au.org.ala.biocache.persistence.Cassandra3PersistenceManager
import org.apache.commons.io.FileUtils
import org.apache.solr.core.{SolrConfig, SolrResourceLoader}
import org.apache.solr.schema.IndexSchemaFactory
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//
///**
// * A data exporter that be used in a threaded manner.
// *
// * @param centralCounter
// * @param threadId
// * @param startKey
// * @param endKey
// * @param columns
// */
//class ColumnExporter(centralCounter: Counter, threadId: Int, startKey: String, endKey: String, columns: List[String], includeRowkey:Boolean, separator:Char = '\t') extends Runnable {
//
//  val logger: Logger = LoggerFactory.getLogger("ColumnExporter")
//
//  def run() {
//
//    val outWriter = new FileWriter(new File( Config.tmpWorkDir + "/fullexport" + threadId + ".txt"))
//    val writer = new CSVWriter(outWriter, separator, '"', '\\')
//    if (includeRowkey) writer.writeNext(Array("rowKey") ++ columns.toArray[String])
//    else writer.writeNext(columns.toArray[String])
//    val start = System.currentTimeMillis
//    var startTime = System.currentTimeMillis
//    var finishTime = System.currentTimeMillis
//    var counter = 0
//    val pageSize = 10000
//    Config.persistenceManager.pageOverSelect("occ", (key, map) => {
//      counter += 1
//      exportRecord(writer, columns, key, map, includeRowkey)
//      if (counter % pageSize == 0 && counter > 0) {
//        centralCounter.addToCounter(pageSize)
//        finishTime = System.currentTimeMillis
//        centralCounter.printOutStatus(threadId, key, "Column Reporter")
//        startTime = System.currentTimeMillis
//      }
//      true
//    }, startKey, endKey, 1000, columns: _*)
//    writer.flush()
//    val fin = System.currentTimeMillis
//    logger.info("[Exporter Thread " + threadId + "] " + counter + " took " + (fin - start).toFloat / 1000f + " seconds")
//  }
//
//  def exportRecord(writer: CSVWriter, fieldsToExport: List[String], guid: String, map: Map[String, String], includeRowkey:Boolean) {
//    val line = if(includeRowkey){
//      Array(guid) ++ (for (field <- fieldsToExport) yield map.getOrElse(field, ""))
//    } else {
//      (for (field <- fieldsToExport) yield map.getOrElse(field, "")).toArray
//    }
//    writer.writeNext(line)
//  }
//}
//
//class BulkColumnExporter(centralCounter: Counter, threadId: Int, startKey: String, endKey: String,
//                         fieldLists: ListBuffer[(Array[String], Array[String], Array[Int], Boolean)],
//                         outputStreams: ArrayBuffer[CSVWriter], filters: ListBuffer[(String, String)]) extends Runnable {
//
//  val elpattern: Regex = """el[0-9]+""".r
//  val clpattern: Regex = """cl[0-9]+""".r
//
//  val logger: Logger = LoggerFactory.getLogger("BulkColumnExporter")
//
//  def run() {
//    val start = System.currentTimeMillis
//    var startTime = System.currentTimeMillis
//    var finishTime = System.currentTimeMillis
//    var counter = 0
//    val pageSize = 10000
//    Config.persistenceManager.pageOverAll("occ", (key, map) => {
//      counter += 1
//      try {
//        if (!map.getOrElse("uuid", "").isEmpty && map.getOrElse(FullRecordMapper.deletedColumn, "").isEmpty) {
//          for (i <- filters.indices) {
//            val v = filters(i)
//            var found = false
//
//            if (v._1.equals("species_group")) {
//              val lft = map.get("left.p")
//              val rgt = map.get("right.p")
//              if (lft.isDefined && rgt.isDefined) {
//                // check the species groups
//                val sgs = SpeciesGroups.getSpeciesGroups(lft.get, rgt.get)
//                if (sgs.isDefined) {
//                  sgs.get.foreach { v1 => if (v._2.equals(v1)) found = true }
//                }
//              }
//            }
//
//            if (found || map.getOrElse(v._1, "").equals(v._2) || ("*".equals(v._2) && !map.getOrElse(v._1, "").isEmpty)) {
//              exportRecord(outputStreams(i), fieldLists(i), key, map)
//            }
//          }
//        }
//      } catch {
//        case e:Exception => logger.error("failed to export: " + key, e);
//      }
//      if (counter % pageSize == 0 && counter > 0) {
//        centralCounter.addToCounter(pageSize)
//        finishTime = System.currentTimeMillis
//        centralCounter.printOutStatus(threadId, key, "BulkColumnExporter")
//        startTime = System.currentTimeMillis
//      }
//      true
//    }, startKey, endKey)
//
//    val fin = System.currentTimeMillis
//    logger.info("[Exporter Thread " + threadId + "] " + counter + " took " + (fin - start).toFloat / 1000f + " seconds")
//  }
//
//  def exportRecord(writer: CSVWriter, fieldsToExport: (Array[String], Array[String], Array[Int], Boolean),
//                   guid: String, map: Map[String, String]) {
//    val (fields, layers, qa, userQa) = fieldsToExport
//    val values = new ArrayBuffer[String]()
//
//    for (i <- fields.indices) {
//      values += map.getOrElse(fields(i), "").replace("\n", "")
//    }
//
//    if (layers != null) {
//      val ly = Json.toStringMap(map.getOrElse("el.p", "{}")) ++ Json.toStringMap(map.getOrElse("cl.p", "{}"))
//      for (i <- layers.indices) {
//        values += ly.getOrElse(layers(i), "").replace("\n", "")
//      }
//    }
//
//    if (qa != null) {
//      //now handle the QA fields
//      val failedCodes = getErrorCodes(map)
//      //work way through the codes and add to output
//      for (i <- qa.indices) {
//        values += failedCodes.contains(qa(i)).toString.replace("\n", "")
//      }
//    }
//
//    if (userQa) {
//      if (map.contains(FullRecordMapper.userQualityAssertionColumn))
//        values += getUserAssertionsString(map.getOrElse("rowKey","").replace("\n", "")) else ""
//    }
//
//    writer.writeNext(values.toArray)
//  }
//
//  def getErrorCodes(map:Map[String, String]):Array[Integer]={
//    val array:Array[List[Integer]] = FullRecordMapper.qaFields.filter(field => map.getOrElse(field, "[]") != "[]").toArray.map(field => {
//      Json.toListWithGeneric(map(field),classOf[java.lang.Integer])
//    }).asInstanceOf[Array[List[Integer]]]
//    if(!array.isEmpty)
//      array.reduceLeft(_++_).toArray
//    else
//      Array()
//  }
//
//  def getUserAssertionsString(rowKey:String):String ={
//    val assertions:List[QualityAssertion] = getUserAssertions(rowKey)
//    val string:StringBuilder = new StringBuilder()
//    assertions.foreach( assertion => {
//      if (assertion != null) {
//        if (string.nonEmpty) string.append('|')
//        //format as ~ delimited created~name~comment~user
//        val comment = {
//          if (assertion.comment != null) {
//            assertion.comment
//          } else {
//            ""
//          }
//        }
//        val userDisplayName = {
//          if (assertion.userDisplayName != null) {
//            assertion.userDisplayName
//          } else {
//            ""
//          }
//        }
//        val formatted = assertion.created + "~" + assertion.name + "~" + comment.replace('~', '-').replace('\n', ' ') + "~" + userDisplayName.replace('~', '-')
//        string.append(formatted.replace('|', '/'))
//      }
//    })
//    string.toString()
//  }
//
//  def getUserAssertions(rowKey:String): List[QualityAssertion] ={
//    val startKey = rowKey + "|"
//    val endKey = startKey + "~"
//    val userAssertions = new ArrayBuffer[QualityAssertion]
//    //page over all the qa's that are for this record
//    Config.persistenceManager.pageOverAll("qa",(guid, map)=>{
//      val qa = new QualityAssertion()
//      qa.referenceRowKey = guid
//      FullRecordMapper.mapPropertiesToObject(qa, map)
//      userAssertions += qa
//      true
//    },startKey, endKey)
//
//    userAssertions.toList
//  }
//}
//
///**
// * A column reporter that reports record counts.
// *
// * @param centralCounter
// * @param threadId
// * @param startKey
// * @param endKey
// */
//class ColumnReporterRunner(centralCounter: Counter, threadId: Int, startKey: String, endKey: String) extends Runnable {
//
//  val logger: Logger = LoggerFactory.getLogger("ColumnReporterRunner")
//  val myset = new mutable.HashSet[String]
//
//  def run() {
//    println("[THREAD " + threadId + "] " + startKey + " TO " + endKey)
//    val start = System.currentTimeMillis
//    var startTime = System.currentTimeMillis
//    var finishTime = System.currentTimeMillis
//    var counter = 0
//    val pageSize = 10000
//    Config.persistenceManager.pageOverAll("occ", (guid, map) => {
//      myset ++= map.keySet
//      counter += 1
//      if (counter % pageSize == 0 && counter > 0) {
//        centralCounter.addToCounter(pageSize)
//        finishTime = System.currentTimeMillis
//        centralCounter.printOutStatus(threadId, guid, "Column Reporter")
//        startTime = System.currentTimeMillis
//      }
//      true
//    }, startKey, endKey)
//    val fin = System.currentTimeMillis
//    logger.info("[Thread " + threadId + "] " + counter + " took " + (fin - start).toFloat / 1000f + " seconds")
//    logger.info("[Thread " + threadId + "] " + myset)
//  }
//}
//
///**
// * A one off class used to repair duplication status properties.
//  *
//  * @param centralCounter
// * @param threadId
// * @param startKey
// * @param endKey
// */
//class RepairRecordsRunner(centralCounter: Counter, threadId: Int, startKey: String, endKey: String) extends Runnable {
//  val logger: Logger = LoggerFactory.getLogger("RepairRecordsRunner")
//  var counter = 0
//
//  def run() {
//    val pageSize = 1000
//    var counter = 0
//    var startTime = System.currentTimeMillis
//    var finishTime = System.currentTimeMillis
//    logger.info("Starting to repair from " + startKey + " to " + endKey)
//    Config.persistenceManager.pageOverSelect("occ", (guid, map) => {
//      counter += 1
//
//      val dstatus = map.getOrElse("duplicationStatus.p", "")
//      if (dstatus.equals("D")) {
//        val qa = Config.occurrenceDAO.getSystemAssertions(guid).find(_.getCode == AssertionCodes.INFERRED_DUPLICATE_RECORD.code)
//        if (qa.isEmpty) {
//          //need to add the QA
//          Config.occurrenceDAO.addSystemAssertion(guid, QualityAssertion(AssertionCodes.INFERRED_DUPLICATE_RECORD, "Record has been inferred as closely related to  " + map.getOrElse("associatedOccurrences.p", "")), checkExisting = false)
//          logger.info("REINDEX:::" + guid)
//        }
//      }
//      if (counter % pageSize == 0 && counter > 0) {
//        centralCounter.addToCounter(pageSize)
//        finishTime = System.currentTimeMillis
//        centralCounter.printOutStatus(threadId, guid, "Repairer")
//        startTime = System.currentTimeMillis
//      }
//      true
//    }, startKey, endKey, pageSize, "qualityAssertion", "rowKey", "uuid", "duplicationStatus.p", "associatedOccurrences.p")
//  }
//
//  val qaphases = Array("loc.qa", "offline.qa", "class.qa", "bor.qa", "type.qa", "attr.qa", "image.qa", "event.qa")
//
//  def sortOutQas(guid: String, list: List[QualityAssertion]): (String, String) = {
//    val failed: Map[String, List[Int]] = list.filter(_.qaStatus == 0).map(_.code).groupBy(qa => Processors.getProcessorForError(qa) + ".qa")
//    val gk = AssertionCodes.isGeospatiallyKosher(failed.getOrElse("loc.qa", List()).toArray).toString
//    val tk = AssertionCodes.isTaxonomicallyKosher(failed.getOrElse("class.qa", List()).toArray).toString
//
//    val empty = qaphases.filterNot(p => failed.contains(p)).map(_ -> "[]")
//    val map = Map("geospatiallyKosher" -> gk, "taxonomicallyKosher" -> tk) ++ failed.filterNot(_._1 == ".qa").map {
//      case (key, value) =>
//        (key, Json.toJSON(value.toArray))
//    } ++ empty
//    //revise the properties in the db
//    Config.persistenceManager.put(guid, "occ", map, newRecord = false, removeNullFields = false)
//
//    //check to see if there is a tool QA and remove one
//    val dupQA = list.filter(_.code == AssertionCodes.INFERRED_DUPLICATE_RECORD.code)
//    //dupQA.foreach(qa => println(qa.getComment))
//    if (dupQA.size > 1) {
//      val newList: List[QualityAssertion] = list.diff(dupQA) ++ List(dupQA.head)
//      //println("Original size " + list.length + "  new size =" + newList.length)
//      Config.persistenceManager.putList(guid, "occ", FullRecordMapper.qualityAssertionColumn, newList, classOf[QualityAssertion], newRecord = false, overwrite = true, deleteIfNullValue = false)
//    }
//
//    (gk, tk)
//  }
//}
//
///**
// * A one off class used to repair datum properties.
//  *
//  * @param centralCounter
// * @param threadId
// * @param startKey
// * @param endKey
// */
//class DatumRecordsRunner(centralCounter: Counter, threadId: Int, startKey: String, endKey: String) extends Runnable {
//  val logger: Logger = LoggerFactory.getLogger("DatumRecordsRunner")
//  val processor = new RecordProcessor
//  var ids = 0
//  val threads = 2
//  var batches = 0
//
//  def run() {
//    val pageSize = 1000
//    var counter = 0
//    var numIssue = 0
//    val start = System.currentTimeMillis
//    var startTime = System.currentTimeMillis
//    var finishTime = System.currentTimeMillis
//
//    logger.info("Starting thread " + threadId + " from " + startKey + " to " + endKey)
//    def locProcess = new LocationProcessor
//    Config.persistenceManager.pageOverSelect("occ", (guid, map) => {
//      counter += 1
//
//      if (StringUtils.isNotBlank(map.getOrElse("geodeticDatum", ""))) {
//        //check the precision of the lat/lon
//        def lat = map.getOrElse("decimalLatitude", "0")
//        def lon = map.getOrElse("decimalLongitude", "0")
//        def locqa = Json.toIntArray(map.getOrElse("loc.qa", "[]"))
//        if (locProcess.getNumberOfDecimalPlacesInDouble(lat) != locProcess.getNumberOfDecimalPlacesInDouble(lon) && locqa.contains(45)) {
//          numIssue += 1
//          logger.info("FIXME from THREAD " + threadId + "\t" + guid)
//        }
//      }
//
//      if (counter % pageSize == 0 && counter > 0) {
//        centralCounter.addToCounter(pageSize)
//        finishTime = System.currentTimeMillis
//        centralCounter.printOutStatus(threadId, guid, "Datum")
//        startTime = System.currentTimeMillis
//      }
//      true
//    }, startKey, endKey, 1000, "decimalLatitude", "decimalLongitude", "rowKey", "uuid", "geodeticDatum", "loc.qa")
//    val fin = System.currentTimeMillis
//    logger.info("[Datum Thread " + threadId + "] " + counter + " took " + (fin - start).toFloat / 1000f + " seconds")
//    logger.info("Finished.")
//  }
//}
//
/////**
////  * A class that can be used to reload sampling values for all records.
////  *
////  * @param centralCounter
////  * @param threadId
////  * @param startKey
////  * @param endKey
////  */
////class AvroExportRunner(centralCounter: Counter, threadId: Int, startKey: String, endKey: String) extends Runnable {
////
////  val logger = LoggerFactory.getLogger("AvroExportRunner")
////
////  val outputDirPath = "/data/avro-export/shard-" + threadId
////
////  val outputDir = new File(outputDirPath)
////
////  FileUtils.forceMkdir(outputDir)
////
////  val schema = AvroUtil.getAvroSchemaForIndex
////  val writer = AvroUtil.getAvroWriter(outputDirPath + "/records.avro")
////  val indexDAO = new SolrIndexDAO("","","")
////
////  def run {
////
////    var counter = 0
////
////    val start = System.currentTimeMillis
////    logger.info(s"Starting thread $threadId from $startKey to  $endKey")
////    Config.persistenceManager.pageOverAll("occ", (guid, map) => {
////      try {
////        val doc = indexDAO.generateSolrDocument(guid, map, List(), threadId.toString)
////        if(doc != null){
////          val record = new GenericData.Record(schema)
////          AvroUtil.csvHeader.foreach { field =>
////
////            if (indexDAO.multiValueFields.contains(field)) {
////              //add a multi valued field
////              val fieldValues = doc.getFieldValues(field)
////              if(fieldValues != null && !fieldValues.isEmpty){
////                val list = new util.ArrayList[String]
////                val iter = fieldValues.iterator()
////                while (iter.hasNext){
////                  list.add(iter.next().toString)
////                }
////                record.put(field, list)
////              }
////            } else {
////              val fieldValue = doc.getFieldValue(field)
////              if(fieldValue != null && StringUtils.isNotBlank(fieldValue.toString)){
////                record.put(field, fieldValue.toString)
////              }
////            }
////          }
////          if(record.get("id") != null){
////            writer.append(record)
////          }
////        }
////      } catch {
////        case e:Exception => logger.error(s"Problem indexing record: $guid" +" - error message: " + e.getMessage)
////      }
////
////      counter += 1
////      if(counter % 10000 == 0){
////        writer.flush()
////        logger.info(s"[AvroExportRunner Thread $threadId] Export of data $counter, last key $guid")
////      }
////      true
////    }, startKey, endKey, 1000)
////
////    writer.flush()
////    writer.close()
////    val fin = System.currentTimeMillis
////    val timeTakenInSecs = ((fin - start).toFloat) / 1000f
////    logger.info(s"[AvroExportRunner Thread $threadId] $counter took $timeTakenInSecs seconds")
////  }
////}
////
////
////
///**
// * A class that can be used to reload sampling values for all records.
// *
// * @param centralCounter
// * @param threadId
// */
//class LoadSamplingRunner(centralCounter: Counter, threadId: Int, dataResourceUid: String) extends Runnable {
//
//  val logger: Logger = LoggerFactory.getLogger("LoadSamplingRunner")
//  var ids = 0
//  val threads = 2
//  var batches = 0
//
//  def run() {
//    var counter = 0
//    val start = System.currentTimeMillis
//    logger.info("Starting thread " + threadId + " for " + dataResourceUid)
//    Config.persistenceManager.pageOverSelect("occ", (guid, map) => {
//      val lat = map.getOrElse("decimalLatitude" +Config.persistenceManager.fieldDelimiter+ "p","")
//      val lon = map.getOrElse("decimalLongitude" +Config.persistenceManager.fieldDelimiter + "p" ,"")
//      if(lat != null && lon != null){
//        val point = LocationDAO.getSamplesForLatLon(lat, lon)
//        if(point.isDefined){
//          val (location, environmentalLayers, contextualLayers) = point.get
//          Config.persistenceManager.put(guid, "occ", Map(
//                      "el" + Config.persistenceManager.fieldDelimiter+ "p" -> Json.toJSON(environmentalLayers),
//                      "cl" + Config.persistenceManager.fieldDelimiter+ "p" -> Json.toJSON(contextualLayers)), newRecord = false, removeNullFields = false)
//        }
//        counter += 1
//        if(counter % 10000 == 0){
//          logger.info("[LoadSamplingRunner Thread " + threadId + "] Import of sample data " + counter + " Last key " + guid)
//        }
//      }
//      true
//    }, "dataResourceUid", dataResourceUid, 1000, "decimalLatitude" + Config.persistenceManager.fieldDelimiter+ "p", "decimalLongitude" +Config.persistenceManager.fieldDelimiter+"p" )
//    val fin = System.currentTimeMillis
//    logger.info("[LoadSamplingRunner Thread " + threadId + "] " + counter + " took " + (fin - start).toFloat / 1000f + " seconds")
//    logger.info("Finished.")
//  }
//}
//
///**
// * A class that can be used to reprocess all records in a threaded manner.
// *
// * @param centralCounter
// * @param threadId
// */
//class ProcessRecordsRunner(centralCounter: Counter, threadId: Int, dataResourceUid: String) extends Runnable {
//  val logger: Logger = LoggerFactory.getLogger("ProcessRecordsRunner")
//  val processor = new RecordProcessor
//  var ids = 0
//  val threads = 2
//  var batches = 0
//
//  def getValue(field: String, map: scala.collection.Map[String, String]): String = map.getOrElse(field.toLowerCase, "")
//
//  def getValue(field: String, map: scala.collection.Map[String, String], default:String): String = map.getOrElse(field.toLowerCase, default)
//
//  def getParsedValue(field: String, map: scala.collection.Map[String, String]): String = getValue(field + Config.persistenceManager.fieldDelimiter + "p", map)
//
//  def getParsedValue(field: String, map: scala.collection.Map[String, String], default:String): String = {
//    val value = getValue(field + Config.persistenceManager.fieldDelimiter + "p", map)
//    if(value == ""){
//      default
//    } else {
//      value
//    }
//  }
//
//  def hasParsedValue(field: String, map: scala.collection.Map[String, String]): Boolean = getValue(field + Config.persistenceManager.fieldDelimiter + "p", map) != ""
//
//  def getValue(field: String, map: scala.collection.Map[String, String], default:String  = "", checkParsed: Boolean): String = {
//    val value = getValue(field, map)
//    if (value == "" && checkParsed) {
//      getValue(field + Config.persistenceManager.fieldDelimiter + "p", map, default)
//    } else {
//      value
//    }
//  }
//
//  def run() {
//    var counter = 0
//    val start = System.currentTimeMillis
//    var startTime = System.currentTimeMillis
//    var finishTime = System.currentTimeMillis
//    //var buff = new ArrayBuffer[(FullRecord,FullRecord)]
//    println("Starting thread " + threadId + " for " + dataResourceUid)
//    val batches = new scala.collection.mutable.ListBuffer[Map[String, Object]]
//    val batchSize = 200
//    Config.occurrenceDAO.pageOverRawProcessed(rawAndProcessed => {
//      val updates = mutable.Map[String, String]()
////      val taxonProfileWithOption = TaxonProfileDAO.getByGuid(getParsedValue("taxonConceptID", rawAndProcessed._1.get))
////      if(!taxonProfileWithOption.isEmpty){
////        val taxonProfile = taxonProfileWithOption.get
////        //add the conservation status if necessary
////        if (taxonProfile.conservation != null) {
////          val country = taxonProfile.retrieveConservationStatus(map.getOrElse("country.p", ""))
////          updates.put("countryConservation.p", country.getOrElse(""))
////          val state = taxonProfile.retrieveConservationStatus(map.getOrElse("stateProvince.p", ""))
////          updates.put("stateConservation.p", state.getOrElse(""))
////          val global = taxonProfile.retrieveConservationStatus("Global")
////          updates.put("Global", global.getOrElse(""))
////        }
////      }
//      if (updates.size < 3) {
//        updates.put("countryConservation.p", "")
//        updates.put("stateConservation.p", "")
//        updates.put("Global", "")
//      }
////      val changes = updates.filter(it => map.getOrElse(it._1, "") != it._2)
////      if (!changes.isEmpty) {
////        Config.persistenceManager.put(guid, "occ", changes.toMap, true)
////      }
//      counter += 1
////      if(counter % 10000 == 0){
////        logger.info("[LoadTaxonConservationData Thread " + threadId + "] Import of sample data " + counter + " Last key " + guid)
////      }
//
//      true
//    }, dataResourceUid)
//    if (batches.nonEmpty) {
//      processor.writeProcessBatch(batches.toList)
//    }
//    val fin = System.currentTimeMillis
//    logger.info("[LoadTaxonConservationData Thread " + threadId + "] " + counter + " took " + (fin - start).toFloat / 1000f + " seconds")
//    logger.info("Finished.")
//  }
//}
//
///**
//  * A runnable thread for creating a complete new index.
//  *
//  * tuning:
//  *
//  * queues and controlling config:
//  * for each thread(--thread) there is a stack of queues
//  * Cassandra(--pagesize) ->
//  * Processing(solr.batch.size) ->
//  * LuceneDocuments(solr.hard.commit.size) ->
//  * CommitBatch(solr.hard.commit.size / (writerthreads + 1))
//  *
//  * --processorBufferSize can be small, (100). Ideally it is the same as --pagesize, memory permitting.
//  *
//  * --writterBufferSize should be large (10000) to reduce commit batch overhead.
//  *
//  * --writerram (default=200 MB) is large to reduce the number of disk flushes. The impact can be observed
//  * comparing 'index docs committed/in ram' values. Note that memory required is --writerram * --threads MB.
//  * The lesser size of --writeram to exceed is that it is large enough to
//  * fit --writterBufferSize / (writerthreads + 2) documents. The average doc size
//  * can be determined from 'in ram/ram MB'. The larger --writerram is, the less merging that will be required
//  * when finished.
//  *
//  * --pagesize should be adjusted for cassandra read performance (1000)
//  *
//  * --threads can be reduced but this may have the largest impact on performance. Compare 'average records/s'
//  * for different settings.
//  *
//  * --threads increase should more quickly add documents to the Processing queue, if it is consistently low.
//  * This has a large impact on memory required.
//  *
//  * --processthreads should be increased if the Processing queue is consistently full. Also depends on available CPU.
//  *
//  * --writerthreads may be increased if the LuceneDocuments queue is consistently full. This has low impact on
//  * writer performance. Also depends on available CPU.
//  *
//  * --writersegmentsize should not be low because more segments are produced and will need to be merged at the end of
//  *indexing. If it is too large the performance on producing the lucene index diminish over time, for each --thread.
//  *
//  * To run indexing without the queues Processing, LuceneDocuments, CommitBatch and their associated threads,
//  * use --processthreads=0 and --writerthreads=0. This is for low mem/slow disk/low number of CPU systems.
//  *
//  * After adjusting the number of threads, the bottleneck; cassandra, processing or lucene, can be observed with
//  * cassandraTime, processingTime, and solrTime or the corresponding queue sizes.
//  *
//  * @param centralCounter
//  * @param confDirPath
//  * @param pageSize
//  * @param luceneIndexing
//  * @param processingThreads
//  * @param processorBufferSize
//  * @param singleWriter
//  */
//class IndexRunner(centralCounter: Counter,
//                  confDirPath: String, pageSize: Int = 200,
//                  luceneIndexing: ArrayBuffer[LuceneIndexing] = null,
//                  processingThreads: Integer = 1,
//                  processorBufferSize: Integer = 100,
//                  singleWriter: Boolean = false,
//                  test: Boolean = false,
//                  numThreads: Int = 2) extends Runnable {
//
//  val logger: Logger = LoggerFactory.getLogger("IndexRunner")
//
//  val startTimeFinal: Long = System.currentTimeMillis()
//
//  val directoryList = new java.util.ArrayList[File]
//
//  val timing = new AtomicLong(0)
//
//  val threadId = 0
//
//  def run() {
//
//    //need to synchronize luceneIndexing for docBuilder.index() when indexer.commitThreadCount == 0
//    val lock : Array[Object] = new Array[Object](luceneIndexing.size)
//    for (i <- luceneIndexing.indices) {
//      lock(i) = new Object()
//    }
//
//    //solr-create/thread-0/conf
//    val newIndexDir = new File(confDirPath)
//
//    val indexer = new SolrIndexDAO(newIndexDir.getParentFile.getParent, Config.excludeSensitiveValuesFor, Config.extraMiscFields)
//
//    var counter = 0
//    val start = System.currentTimeMillis
//    var startTime = System.currentTimeMillis
//    var finishTime = System.currentTimeMillis
//
//    val csvFileWriter = if (Config.exportIndexAsCsvPath.length > 0) {
//      indexer.getCsvWriter()
//    } else {
//      null
//    }
//    val csvFileWriterSensitive = if (Config.exportIndexAsCsvPath.length > 0) {
//      indexer.getCsvWriter(true)
//    } else {
//      null
//    }
//
//    val queue: LinkedBlockingQueue[(String, GettableData, ColumnDefinitions)] = new LinkedBlockingQueue[(String, GettableData, ColumnDefinitions)](processorBufferSize)
//
//    val threads = mutable.ArrayBuffer[ProcessThread]()
//
//    if (processingThreads > 0 && luceneIndexing != null) {
//      for (i <- 0 until processingThreads) {
//        var t = new ProcessThread()
//        t.start()
//        threads += t
//      }
//    }
//
//    class ProcessThread extends Thread() {
//      var recordsProcessed = 0
//
//      override def run() {
//
//        // Specify the SOLR config to use
//        indexer.solrConfigPath = newIndexDir.getAbsolutePath + "/solrconfig.xml"
//        var continue = true
//        while (continue) {
//          val m: (String, GettableData, ColumnDefinitions) = queue.take()
//          if (m._1 == null) {
//            continue = false
//          } else {
//            try {
//              val t1 = System.nanoTime()
//
//              //evenly distribute records between LuceneIndexing objects
//              //use 50000 offset per index
//              val idx = recordsProcessed % luceneIndexing.size
//
//              val t2 = indexer.indexFromArray(m._1, m._2, m._3,
//                docBuilder = luceneIndexing(idx).getDocBuilder,
//                lock = lock(idx),
//                test = test)
//
//              //if a docbuilder has no free docs, try the next docbuilder
////              var idx = firstIdx
////              var retry = true
////              while(retry) {
////                val docBuilder = luceneIndexing(idx).getDocBuilder
////                if (docBuilder.tryNewDoc()) {
////                  val t2 = indexer.indexFromArray(m._1, m._2, m._3,
////                    docBuilder = luceneIndexing(idx).getDocBuilder,
////                    lock = lock(idx),
////                    test = test)
////
////                  timing.addAndGet(System.nanoTime() - t1 - t2)
////                  retry = false
////                } else {
////                  if (idx + 1 >= luceneIndexing.size) {
////                    idx = 0
////                  } else {
////                    idx = idx + 1
////                  }
////                }
////              }
//            } catch {
//              case e: InterruptedException => throw e
//              case e: Exception => logger.error("guid:" + m._1 + ", " + e.getMessage)
//            }
//          }
//          recordsProcessed = recordsProcessed + 1
//        }
//      }
//    }
//
//    //page through and create and index for this range
//    val t2Total = new AtomicLong(0L)
//    var t2 = System.nanoTime()
//    var uuidIdx = -1
//    Config.persistenceManager.asInstanceOf[Cassandra3PersistenceManager].pageOverLocalV2("occ", (guid, row, columnDefinitions, _) => {
//      t2Total.addAndGet(System.nanoTime() - t2)
//
//      counter += 1
//      //ignore the record if it has the guid that is the startKey this is because it will be indexed last by the previous thread.
//      try {
//        if (uuidIdx == -1) {
//          uuidIdx = columnDefinitions.getIndexOf("uuid")
//        }
//        if (!StringUtils.isEmpty(row.getString(uuidIdx))) {
//          val t1 = System.nanoTime()
//          var t2 = 0L
//          if (processingThreads > 0) queue.put((row.getString(uuidIdx), row, columnDefinitions))
//          else t2 = indexer.indexFromArray(row.getString(uuidIdx), row, columnDefinitions)
//
//          timing.addAndGet(System.nanoTime() - t1 - t2)
//        }
//      } catch {
//        case e: Exception =>
//          logger.error("Problem indexing record: " + guid + " " + e.getMessage, e)
//          if (logger.isDebugEnabled) {
//            logger.debug("Error during indexing: " + e.getMessage, e)
//          }
//      }
//
//      if (counter % pageSize * 10 == 0 && counter > 0) {
//        centralCounter.addToCounter(pageSize)
//        finishTime = System.currentTimeMillis
//        centralCounter.printOutStatus(threadId, guid, "Indexer", startTimeFinal)
//
//
//        logger.info("cassandraTime(s)=" + t2Total.get() / 1000000000 +
//          ", processingTime[" + processingThreads + "](s)=" + timing.get() / 1000000000 +
//          ", solrTime[" + luceneIndexing(0).getThreadCount + "](s)=" + luceneIndexing(0).getTiming / 1000000000 +
//          ", index docs committed/in ram/ram MB=" +
//          luceneIndexing(0).getCount + "/" + luceneIndexing(0).ramDocs() + "/" + (luceneIndexing(0).ramBytes() / 1024 / 1024) +
//          ", mem free(Mb)=" + Runtime.getRuntime.freeMemory() / 1024 / 1024 +
//          ", mem total(Mb)=" + Runtime.getRuntime.maxMemory() / 1024 / 1024 +
//          ", queues (processing/lucene docs/commit batch) " + queue.size() + "/" + luceneIndexing(0).getQueueSize + "/" + luceneIndexing(0).getBatchSize)
//      }
//
//      startTime = System.currentTimeMillis
//
//      t2 = System.nanoTime()
//
//      //counter < 2000
//      true
//    }, pageSize, numThreads, Array())
//
//    //final log entry
//    centralCounter.printOutStatus(threadId, "", "Indexer", startTimeFinal)
//
//    logger.info("FINAL >>> cassandraTime(s)=" + t2Total.get() / 1000000000 +
//      ", processingTime[" + processingThreads + "](s)=" + timing.get() / 1000000000 +
//      ", solrTime[" + luceneIndexing(0).getThreadCount + "](s)=" + luceneIndexing(0).getTiming / 1000000000 +
//      ", index docs committed/in ram/ram MB=" +
//      luceneIndexing(0).getCount + "/" + luceneIndexing(0).ramDocs() + "/" + (luceneIndexing(0).ramBytes() / 1024 / 1024) +
//      ", mem free(Mb)=" + Runtime.getRuntime.freeMemory() / 1024 / 1024 +
//      ", mem total(Mb)=" + Runtime.getRuntime.maxMemory() / 1024 / 1024 +
//      ", queues (processing/lucene docs/commit batch) " + queue.size() + "/" + luceneIndexing(0).getQueueSize + "/" + luceneIndexing(0).getBatchSize)
//
//    if (csvFileWriter != null) {
//      csvFileWriter.flush(); csvFileWriter.close()
//    }
//    if (csvFileWriterSensitive != null) {
//      csvFileWriterSensitive.flush(); csvFileWriterSensitive.close()
//    }
//
//    //signal threads to end
//    for (i <- 0 until processingThreads) {
//      queue.put((null, null, null))
//    }
//
//    //wait for threads to end
//    threads.foreach(t => t.join())
//
//    finishTime = System.currentTimeMillis
//    logger.info("Total indexing time for this thread " + (finishTime - start).toFloat / 60000f + " minutes.")
//
//    //close and merge the lucene index parts
//    if (luceneIndexing != null && !singleWriter) {
//      for (i <- luceneIndexing.indices) {
//        luceneIndexing(i).close(true, false)
//      }
//    }
//  }
//}
//
//class IndexRunnerMap(centralCounter: Counter,
//                  confDirPath: String, pageSize: Int = 200,
//                  luceneIndexing: ArrayBuffer[LuceneIndexing] = null,
//                  processingThreads: Integer = 1,
//                  processorBufferSize: Integer = 100,
//                  singleWriter: Boolean = false,
//                  test: Boolean = false,
//                  numThreads: Int = 2) extends Runnable {
//
//  val logger: Logger = LoggerFactory.getLogger("IndexRunner")
//
//  val startTimeFinal: Long = System.currentTimeMillis()
//
//  val directoryList = new java.util.ArrayList[File]
//
//  val timing = new AtomicLong(0)
//
//  val threadId = 0
//
//  def run() {
//
//    //need to synchronize luceneIndexing for docBuilder.index() when indexer.commitThreadCount == 0
//    val lock : Array[Object] = new Array[Object](luceneIndexing.size)
//    for (i <- luceneIndexing.indices) {
//      lock(i) = new Object()
//    }
//
//    //solr-create/thread-0/conf
//    val newIndexDir = new File(confDirPath)
//
//    val indexer = new SolrIndexDAO(newIndexDir.getParentFile.getParent, Config.excludeSensitiveValuesFor, Config.extraMiscFields)
//
//    var counter = 0
//    val start = System.currentTimeMillis
//    var startTime = System.currentTimeMillis
//    var finishTime = System.currentTimeMillis
//
//    val csvFileWriter = if (Config.exportIndexAsCsvPath.length > 0) {
//      indexer.getCsvWriter()
//    } else {
//      null
//    }
//    val csvFileWriterSensitive = if (Config.exportIndexAsCsvPath.length > 0) {
//      indexer.getCsvWriter(true)
//    } else {
//      null
//    }
//
//    val queue: LinkedBlockingQueue[(String, Map[String, String])] = new LinkedBlockingQueue[(String, Map[String, String])](processorBufferSize)
//
//    val threads = mutable.ArrayBuffer[ProcessThread]()
//
//    if (processingThreads > 0 && luceneIndexing != null) {
//      for (i <- 0 until processingThreads) {
//        var t = new ProcessThread()
//        t.start()
//        threads += t
//      }
//    }
//
//    class ProcessThread extends Thread() {
//      var recordsProcessed = 0
//
//      override def run() {
//
//        // Specify the SOLR config to use
//        indexer.solrConfigPath = newIndexDir.getAbsolutePath + "/solrconfig.xml"
//        var continue = true
//        while (continue) {
//          val m: (String, Map[String, String]) = queue.take()
//          if (m._1 == null) {
//            continue = false
//          } else {
//            try {
//              val t1 = System.nanoTime()
//
//              //evenly distribute records between LuceneIndexing objects
//              val idx = recordsProcessed % luceneIndexing.size
//
//              val t2 = indexer.indexFromMapNew(m._1, m._2,
//                docBuilder = luceneIndexing(idx).getDocBuilder,
//                lock = lock(idx),
//                test = test)
//
//              timing.addAndGet(System.nanoTime() - t1 - t2)
//
//              //if a docbuilder has no free docs, try the next docbuilder
////              var idx = firstIdx
////              var retry = true
////              while(retry) {
////                val docBuilder = luceneIndexing(idx).getDocBuilder
////                if (docBuilder.tryNewDoc()) {
////                  val t2 = indexer.indexFromMapNew(m._1, m._2,
////                    docBuilder = luceneIndexing(idx).getDocBuilder,
////                    lock = lock(idx),
////                    test = test)
////
////                  timing.addAndGet(System.nanoTime() - t1 - t2)
////                  retry = false
////                } else {
////                  if (idx + 1 >= luceneIndexing.size) {
////                    idx = 0
////                  } else {
////                    idx = idx + 1
////                  }
////                }
////              }
//            } catch {
//              case e: InterruptedException => throw e
//              case e: Exception => logger.error("guid:" + m._1 + ", " + e.getMessage)
//            }
//          }
//          recordsProcessed = recordsProcessed + 1
//        }
//      }
//    }
//
//    //page through and create and index for this range
//    val t2Total = new AtomicLong(0L)
//    var t2 = System.nanoTime()
//    var uuidIdx = -1
//    Config.persistenceManager.asInstanceOf[Cassandra3PersistenceManager].pageOverLocal("occ", (guid, map, _) => {
//      t2Total.addAndGet(System.nanoTime() - t2)
//
//      counter += 1
//      //ignore the record if it has the guid that is the startKey this is because it will be indexed last by the previous thread.
//      try {
//        val uuid = map.getOrElse("uuid", "")
//        if (!StringUtils.isEmpty(uuid)) {
//          val t1 = System.nanoTime()
//          var t2 = 0L
//          val idx = counter % luceneIndexing.size
//
//          if (processingThreads > 0) queue.put((uuid, map))
//          else t2 = indexer.indexFromMapNew(uuid, map,
//            docBuilder = luceneIndexing(0).getDocBuilder,
//            lock = lock(0),
//            test = test)
//
//          timing.addAndGet(System.nanoTime() - t1 - t2)
//        }
//      } catch {
//        case e: Exception =>
//          logger.error("Problem indexing record: " + guid + " " + e.getMessage, e)
//          if (logger.isDebugEnabled) {
//            logger.debug("Error during indexing: " + e.getMessage, e)
//          }
//      }
//
//      if (counter % pageSize * 10 == 0 && counter > 0) {
//        centralCounter.addToCounter(pageSize)
//        finishTime = System.currentTimeMillis
//        centralCounter.printOutStatus(threadId, guid, "Indexer", startTimeFinal)
//
//
//        logger.info("cassandraTime(s)=" + t2Total.get() / 1000000000 +
//          ", processingTime[" + processingThreads + "](s)=" + timing.get() / 1000000000 +
//          ", solrTime[" + luceneIndexing(0).getThreadCount + "](s)=" + luceneIndexing(0).getTiming / 1000000000 +
//          ", index docs committed/in ram/ram MB=" +
//          luceneIndexing(0).getCount + "/" + luceneIndexing(0).ramDocs() + "/" + (luceneIndexing(0).ramBytes() / 1024 / 1024) +
//          ", mem free(Mb)=" + Runtime.getRuntime.freeMemory() / 1024 / 1024 +
//          ", mem total(Mb)=" + Runtime.getRuntime.maxMemory() / 1024 / 1024 +
//          ", queues (processing/lucene docs/commit batch) " + queue.size() + "/" + luceneIndexing(0).getQueueSize + "/" + luceneIndexing(0).getBatchSize)
//      }
//
//      startTime = System.currentTimeMillis
//
//      t2 = System.nanoTime()
//
//      //counter < 2000
//      true
//    }, numThreads, Array())
//
//    //final log entry
//    centralCounter.printOutStatus(threadId, "", "Indexer", startTimeFinal)
//
//    logger.info("FINAL >>> cassandraTime(s)=" + t2Total.get() / 1000000000 +
//      ", processingTime[" + processingThreads + "](s)=" + timing.get() / 1000000000 +
//      ", solrTime[" + luceneIndexing(0).getThreadCount + "](s)=" + luceneIndexing(0).getTiming / 1000000000 +
//      ", index docs committed/in ram/ram MB=" +
//      luceneIndexing(0).getCount + "/" + luceneIndexing(0).ramDocs() + "/" + (luceneIndexing(0).ramBytes() / 1024 / 1024) +
//      ", mem free(Mb)=" + Runtime.getRuntime.freeMemory() / 1024 / 1024 +
//      ", mem total(Mb)=" + Runtime.getRuntime.maxMemory() / 1024 / 1024 +
//      ", queues (processing/lucene docs/commit batch) " + queue.size() + "/" + luceneIndexing(0).getQueueSize + "/" + luceneIndexing(0).getBatchSize)
//
//    if (csvFileWriter != null) {
//      csvFileWriter.flush(); csvFileWriter.close()
//    }
//    if (csvFileWriterSensitive != null) {
//      csvFileWriterSensitive.flush(); csvFileWriterSensitive.close()
//    }
//
//    //signal threads to end
//    for (i <- 0 until processingThreads) {
//      queue.put((null, null))
//    }
//
//    //wait for threads to end
//    threads.foreach(t => t.join())
//
//    finishTime = System.currentTimeMillis
//    logger.info("Total indexing time for this thread " + (finishTime - start).toFloat / 60000f + " minutes.")
//
//    //close and merge the lucene index parts
//    if (luceneIndexing != null && !singleWriter) {
//      for (i <- luceneIndexing.indices) {
//        luceneIndexing(i).close(true, false)
//      }
//    }
//  }
//}

/**
  * A class that can be used to reload taxon conservation values for all records.
  *
  * @param centralCounter
  * @param threadId
  */
class LoadTaxonConservationData(centralCounter: Counter, threadId: Int) extends Runnable {

  val logger = LoggerFactory.getLogger("LoadTaxonConservationData")
  var ids = 0
  val threads = 2
  var batches = 0

  def run {
    var counter = 0
    val start = System.currentTimeMillis
    val sep = Config.persistenceManager.fieldDelimiter

    val batch: mutable.Map[String, Map[String, String]] = mutable.Map[String, Map[String, String]]()

    Config.persistenceManager.asInstanceOf[Cassandra3PersistenceManager].pageOverLocal("occ", (guid, map, _) => {
      val updates = mutable.Map[String, String]()
      val taxonProfileWithOption = TaxonProfileDAO.getByGuid(map.getOrElse("taxonConceptID" + sep + "p", ""))
      if (!taxonProfileWithOption.isEmpty) {
        val taxonProfile = taxonProfileWithOption.get
        //add the conservation status if necessary
        if (taxonProfile.conservation != null) {
          val country = taxonProfile.retrieveConservationStatus(map.getOrElse("country" + sep + "p", ""))
          updates.put("countryConservation" + sep + "p", country.getOrElse(""))
          val state = taxonProfile.retrieveConservationStatus(map.getOrElse("stateProvince" + sep + "p", ""))
          updates.put("stateConservation" + sep + "p", state.getOrElse(""))
          val global = taxonProfile.retrieveConservationStatus("global")
          updates.put("global", global.getOrElse(""))
        }
      }
      if (updates.size < 3) {
        updates.put("countryConservation" + sep + "p", "")
        updates.put("stateConservation" + sep + "p", "")
        updates.put("global", "")
      }
      val changes = updates.filter(it => map.getOrElse(it._1, "") != it._2)
      if (!changes.isEmpty) {
        batch.put(guid, updates.toMap)
      }

      counter += 1
      if (counter % 10000 == 0) {
        logger.info("[LoadTaxonConservationData Thread " + threadId + "] Import of sample data " + counter + " Last key " + guid)

        if (!batch.isEmpty) {
          logger.info("writing")
          Config.persistenceManager.putBatch("occ", batch.toMap, true, false)
          batch.clear()
        }
      }

      true
    }, 4, Array("taxonConceptID" + sep + "p", "country" + sep + "p", "countryConservation" + sep + "p", "stateProvince" + sep + "p", "stateConservation" + sep + "p", "global"))

    if (!batch.isEmpty) {
      logger.info("writing")
      Config.persistenceManager.putBatch("occ", batch.toMap, true, false)
      batch.clear()
    }

    val fin = System.currentTimeMillis
    logger.info("[LoadTaxonConservationData Thread " + threadId + "] " + counter + " took " + ((fin - start).toFloat) / 1000f + " seconds")
    logger.info("Finished.")
  }
}

/**
  * A class for local records indexing.
  */
class IndexLocalRecordsV2 {

  val logger: Logger = LoggerFactory.getLogger("IndexLocalRecordsV2")

  def indexRecords(numThreads: Int, solrHome: String, solrConfigXmlPath: String, optimise: Boolean, optimiseOnly: Boolean,
                   checkpointFile: String, threadsPerWriter: Int, threadsPerProcess: Int, ramPerWriter: Int,
                   writerSegmentSize: Int, processorBufferSize: Int, writerBufferSize: Int,
                   pageSize: Int, mergeSegments: Int, test: Boolean, writerCount: Int, testMap: Boolean
                  ): Unit = {

    val start = System.currentTimeMillis()

    System.setProperty("tokenRangeCheckPointFile", checkpointFile)

    var count = 0
    val singleWriter = writerCount == 0

    ///init for luceneIndexing
    var luceneIndexing: ArrayBuffer[LuceneIndexing] = new ArrayBuffer[LuceneIndexing]

    val confDir = solrHome + "/solr-create/biocache/conf"
    //solr-create/thread-0/conf
    val newIndexDir = new File(confDir)
    if (newIndexDir.exists) {
      FileUtils.deleteDirectory(newIndexDir.getParentFile)
    }
    FileUtils.forceMkdir(newIndexDir)

    //CREATE a copy of SOLR home
    val sourceConfDir = new File(solrConfigXmlPath).getParentFile
    FileUtils.copyDirectory(sourceConfDir, newIndexDir)

    //identify the first valid schema
    val s1 = new File(confDir + "/schema.xml")
    val s2 = new File(confDir + "/schema.xml.bak")
    val s3 = new File(confDir + "/schema-managed")
    if (s3.exists()) {
      s3.delete()
    }
    val schemaFile: File = {
      if (s1.exists()) {
        s1
      } else {
        s2
      }
    }
    val schema = IndexSchemaFactory.buildIndexSchema(schemaFile.getName,
      SolrConfig.readFromResourceLoader(new SolrResourceLoader(new File(solrHome + "/solr-create/biocache").toPath), "solrconfig.xml"))

    FileUtils.writeStringToFile(new File(solrHome + "/solr-create/solr.xml"), "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><solr></solr>")
    FileUtils.writeStringToFile(new File(solrHome + "/solr-create/zoo.cfg"), "")

    if (singleWriter) {
      luceneIndexing += new LuceneIndexing(schema, writerSegmentSize.toLong, newIndexDir.getParent + "/data0-",
        ramPerWriter, writerBufferSize, writerBufferSize / 2, threadsPerWriter)
    } else {
      for (i <- 0 until writerCount) {
        luceneIndexing += new LuceneIndexing(schema, writerSegmentSize.toLong, newIndexDir.getParent + "/data" + i + "-",
          ramPerWriter, writerBufferSize, writerBufferSize / (threadsPerWriter + 2), threadsPerWriter)
      }
    }

    if (test) {
      DocBuilder.setIsIndexing(false)
    }

    val counter: Counter = new DefaultCounter()
    if (testMap) {
      new IndexRunnerMap(counter,
        confDir,
        pageSize,
        luceneIndexing,
        threadsPerProcess,
        processorBufferSize,
        singleWriter, test, numThreads
      ).run()
    } else {
      new IndexRunner(counter,
        confDir,
        pageSize,
        luceneIndexing,
        threadsPerProcess,
        processorBufferSize,
        singleWriter, test, numThreads
      ).run()
    }

    val end = System.currentTimeMillis()
    logger.info("Indexing completed. Total indexed : " + counter.counter + " in " + ((end - start).toFloat / 1000f / 60f) + " minutes")

    val dirs = new ArrayBuffer[String]()

    if (singleWriter) {
      luceneIndexing(0).close(true, false)
    }
    for (i <- luceneIndexing.indices) {
      for (j <- 0 until luceneIndexing(i).getOutputDirectories.size()) {
        dirs += luceneIndexing(i).getOutputDirectories.get(j).getPath
      }
    }

    luceneIndexing = null
    System.gc()

    val mem = Math.max((Runtime.getRuntime.freeMemory() * 0.75) / 1024 / 1024, writerCount * ramPerWriter).toInt

    //insert new fields into the schema file 's1'
    val s: File = {
      if (s1.exists()) {
        s1
      } else {
        s2
      }
    }
    if (DocBuilder.getAdditionalSchemaEntries.size() > 0) {
      logger.info("Writing " + DocBuilder.getAdditionalSchemaEntries.size() + " new fields into updated schema: " + s1.getPath)
      val schemaString = FileUtils.readFileToString(s)
      //      FileUtils.writeStringToFile(s1, schemaString.replace("</schema>",
      //        StringUtils.join(DocBuilder.getAdditionalSchemaEntries, "\n") + "\n</schema>"))

      //backup and overwrite source schema
      //      FileUtils.copyFile(new File(sourceConfDir + "/schema.xml"),
      //        new File(sourceConfDir + "/schema.xml." + System.currentTimeMillis()))
      //      FileUtils.copyFile(s1, new File(sourceConfDir + "/schema.xml"))

      //remove 'bad' entries
      val sb = new StringBuilder()
      for (i: String <- DocBuilder.getAdditionalSchemaEntries.asScala) {
        if (!i.contains("name=\"\"") && !i.contains("name=\"_\")")) {
          sb.append(i)
          sb.append('\n')
        }
      }

      //export additional fields to a separate file
      FileUtils.writeStringToFile(new File(sourceConfDir + "/additionalFields.list"),
        sb.toString())
    } else {
      FileUtils.writeStringToFile(new File(sourceConfDir + "/additionalFields.list"), "")
    }

    if (mergeSegments > 0) {
      val segmentCount = mergeSegments
      val segmentSize = dirs.length / segmentCount + 1

      logger.info("Merging index into " + segmentCount + " segments. source dirs=" + dirs.length + ", segment size=" + segmentSize + ", mem=" + mem + "mb")

      var dirsRemaining = dirs
      var segmentNumber = 0
      while (dirsRemaining.nonEmpty) {
        var (dirsSegment, remainder) = dirsRemaining.splitAt(Math.min(segmentSize, dirsRemaining.length))
        dirsRemaining = remainder

        logger.info("merged_" + segmentNumber + ", " + dirsSegment)

        IndexMergeTool.merge(solrHome + "merged_" + segmentNumber, dirsSegment.toArray, forceMerge = optimise, dirsSegment.length, deleteSources = false, mem)

        new File(solrHome + "merged_" + segmentNumber + "/conf").mkdirs()
        FileUtils.copyFile(s, new File(solrHome + "merged_" + segmentNumber + "/conf/schema.xml"))

        segmentNumber = segmentNumber + 1
      }
    }

    logger.info("Complete")

    //Move checkpoint file if complete
    new File(checkpointFile).renameTo(new File(checkpointFile + ".complete"))
  }
}
