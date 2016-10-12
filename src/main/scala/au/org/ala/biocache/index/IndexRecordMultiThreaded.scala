package au.org.ala.biocache.index

import java.util

import au.org.ala.biocache._
import org.apache.commons.io.FileUtils
import java.io.File
import scala.io.Source
import java.net.URL
import scala.util.parsing.json.JSON
import scala.collection.mutable.ArrayBuffer
import java.io.FileWriter
import au.com.bytecode.opencsv.CSVWriter
import scala.collection.mutable.HashSet
import org.apache.commons.lang3.StringUtils
import au.org.ala.biocache.processor.{RecordProcessor, Processors, LocationProcessor}
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.vocab.{ErrorCode, AssertionCodes}
import au.org.ala.biocache.util.{AvroUtil, Json, OptionParser}
import au.org.ala.biocache.model.QualityAssertion
import org.slf4j.LoggerFactory
import au.org.ala.biocache.caches.LocationDAO

trait Counter {

  val logger = LoggerFactory.getLogger("Counter")

  var counter = 0

  def addToCounter(amount: Int) = counter += amount

  var startTime = System.currentTimeMillis
  var finishTime = System.currentTimeMillis

  def printOutStatus(threadId: Int, lastKey: String, runnerType: String) = {
    finishTime = System.currentTimeMillis
    logger.info("[" + runnerType + " Thread " + threadId + "] " + counter + " >> Last key : " + lastKey + ", records per sec: " + 1000f / (((finishTime - startTime).toFloat) / 1000f))
    startTime = System.currentTimeMillis
  }
}

/**
 * A trait that will calculate the ranges of rowkeys to use for a multiple threaded process.
 * Each thread can then be assigned a rowkey range to work with independently.
 */
trait RangeCalculator {

  val logger = LoggerFactory.getLogger("RangeCalculator")

  /**
   * For a give webservice URL, calculate a partitioning per thread
   */
  def calculateRanges(threads: Int, query: String = "*:*", start: String = "", end: String = ""): Array[(String, String)] = {

    val firstRequest = Config.biocacheServiceUrl + "/occurrences/search?q=" + query + "&pageSize=1&facet=off&sort=row_key&dir=asc"
    val json = JSON.parseFull(Source.fromURL(new URL(firstRequest)).mkString)
    if (!json.isEmpty && json.get.asInstanceOf[Map[String, Object]].getOrElse("totalRecords", 0).asInstanceOf[Double].toInt > 0) {

      val totalRecords = json.get.asInstanceOf[Map[String, Object]].getOrElse("totalRecords", 0).asInstanceOf[Double].toInt
      logger.info("Total records: " + totalRecords)

      val pageSize = totalRecords.toInt / threads

      var lastKey = start
      val buff = Array.fill(threads)(("", ""))

      for (i <- 0 until threads) {
        val json = JSON.parseFull(Source.fromURL(
          new URL(Config.biocacheServiceUrl + "/occurrences/search?q=" + query + "&facets=row_key&pageSize=0&flimit=1&fsort=index&foffset=" + (i * pageSize))).mkString)

        val facetResults = json.get.asInstanceOf[Map[String, Object]]
          .getOrElse("facetResults", List[Map[String, Object]]())
          .asInstanceOf[List[Map[String, Object]]]

        if (facetResults.size > 0) {
          val rowKey = facetResults.head.get("fieldResult").get.asInstanceOf[List[Map[String, String]]].head.getOrElse("label", "")
          logger.info("Retrieved row key: " + rowKey)

          if (i > 0) {
            buff(i - 1) = (lastKey, rowKey)
          }
          //we want the first key to be ""
          if (i != 0)
            lastKey = rowKey
        }
      }

      buff(buff.length - 1) = (lastKey, end)

      buff
    } else {
      Array.fill(1)((start, end))
    }
  }

  /**
   * Format the keys into a set of tuples which will be a set of ranges.
   *
   * @param keys
   * @param start
   * @param end
   * @return
   */
  def generateRanges(keys: Array[String], start: String, end: String): Array[(String, String)] = {
    val buff = new ArrayBuffer[(String, String)]
    var i = 0
    while (i < keys.size) {
      if (i == 0)
        buff += ((start, keys(i)))
      else if (i == keys.size - 1)
        buff += ((keys(i - 1), end))
      else
        buff += ((keys(i - 1), keys(i)))
      i += 1
    }
    buff.toArray[(String, String)]
  }
}

/**
 * A data exporter that be used in a threaded manner.
 *
 * @param centralCounter
 * @param threadId
 * @param startKey
 * @param endKey
 * @param columns
 */
class ColumnExporter(centralCounter: Counter, threadId: Int, startKey: String, endKey: String, columns: List[String], includeRowkey:Boolean, separator:Char = '\t') extends Runnable {

  val logger = LoggerFactory.getLogger("ColumnExporter")

  def run {

    val outWriter = new FileWriter(new File( Config.tmpWorkDir + "/fullexport" + threadId + ".txt"))
    val writer = new CSVWriter(outWriter, separator, '"', '\\')
    if (includeRowkey) writer.writeNext(Array("rowKey") ++ columns.toArray[String])
    else writer.writeNext(columns.toArray[String])
    val start = System.currentTimeMillis
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis
    var counter = 0
    val pageSize = 10000
    Config.persistenceManager.pageOverSelect("occ", (key, map) => {
      counter += 1
      exportRecord(writer, columns, key, map, includeRowkey)
      if (counter % pageSize == 0 && counter > 0) {
        centralCounter.addToCounter(pageSize)
        finishTime = System.currentTimeMillis
        centralCounter.printOutStatus(threadId, key, "Column Reporter")
        startTime = System.currentTimeMillis
      }
      true
    }, startKey, endKey, 1000, columns: _*)
    writer.flush()
    val fin = System.currentTimeMillis
    logger.info("[Exporter Thread " + threadId + "] " + counter + " took " + ((fin - start).toFloat) / 1000f + " seconds")
  }

  def exportRecord(writer: CSVWriter, fieldsToExport: List[String], guid: String, map: Map[String, String], includeRowkey:Boolean) {
    val line = if(includeRowkey){
      Array(guid) ++ (for (field <- fieldsToExport) yield map.getOrElse(field, ""))
    } else {
      (for (field <- fieldsToExport) yield map.getOrElse(field, "")).toArray
    }
    writer.writeNext(line)
  }
}

/**
 * A column reporter that reports record counts.
 *
 * @param centralCounter
 * @param threadId
 * @param startKey
 * @param endKey
 */
class ColumnReporterRunner(centralCounter: Counter, threadId: Int, startKey: String, endKey: String) extends Runnable {

  val logger = LoggerFactory.getLogger("ColumnReporterRunner")
  val myset = new HashSet[String]

  def run {
    println("[THREAD " + threadId + "] " + startKey + " TO " + endKey)
    val start = System.currentTimeMillis
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis
    var counter = 0
    val pageSize = 10000
    Config.persistenceManager.pageOverAll("occ", (guid, map) => {
      myset ++= map.keySet
      counter += 1
      if (counter % pageSize == 0 && counter > 0) {
        centralCounter.addToCounter(pageSize)
        finishTime = System.currentTimeMillis
        centralCounter.printOutStatus(threadId, guid, "Column Reporter")
        startTime = System.currentTimeMillis
      }
      true
    }, startKey, endKey, 1000)
    val fin = System.currentTimeMillis
    logger.info("[Thread " + threadId + "] " + counter + " took " + ((fin - start).toFloat) / 1000f + " seconds")
    logger.info("[Thread " + threadId + "] " + myset)
  }
}

/**
 * A one off class used to repair duplication status properties.
  *
  * @param centralCounter
 * @param threadId
 * @param startKey
 * @param endKey
 */
class RepairRecordsRunner(centralCounter: Counter, threadId: Int, startKey: String, endKey: String) extends Runnable {
  val logger = LoggerFactory.getLogger("RepairRecordsRunner")
  var counter = 0

  def run {
    val pageSize = 1000
    var counter = 0
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis
    logger.info("Starting to repair from " + startKey + " to " + endKey)
    Config.persistenceManager.pageOverSelect("occ", (guid, map) => {
      counter += 1

      val dstatus = map.getOrElse("duplicationStatus.p", "")
      if (dstatus.equals("D")) {
        val qa = Config.occurrenceDAO.getSystemAssertions(guid).find(_.getCode == AssertionCodes.INFERRED_DUPLICATE_RECORD.code)
        if (qa.isEmpty) {
          //need to add the QA
          Config.occurrenceDAO.addSystemAssertion(guid, QualityAssertion(AssertionCodes.INFERRED_DUPLICATE_RECORD, "Record has been inferred as closely related to  " + map.getOrElse("associatedOccurrences.p", "")), false, false)
          logger.info("REINDEX:::" + guid)
        }
      }
      if (counter % pageSize == 0 && counter > 0) {
        centralCounter.addToCounter(pageSize)
        finishTime = System.currentTimeMillis
        centralCounter.printOutStatus(threadId, guid, "Repairer")
        startTime = System.currentTimeMillis
      }
      true
    }, startKey, endKey, pageSize, "qualityAssertion", "rowKey", "uuid", "duplicationStatus.p", "associatedOccurrences.p")
  }

  val qaphases = Array("loc.qa", "offline.qa", "class.qa", "bor.qa", "type.qa", "attr.qa", "image.qa", "event.qa")

  def sortOutQas(guid: String, list: List[QualityAssertion]): (String, String) = {
    val failed: Map[String, List[Int]] = list.filter(_.qaStatus == 0).map(_.code).groupBy(qa => Processors.getProcessorForError(qa) + ".qa")
    val gk = AssertionCodes.isGeospatiallyKosher(failed.getOrElse("loc.qa", List()).toArray).toString
    val tk = AssertionCodes.isTaxonomicallyKosher(failed.getOrElse("class.qa", List()).toArray).toString

    val empty = qaphases.filterNot(p => failed.contains(p)).map(_ -> "[]")
    val map = Map("geospatiallyKosher" -> gk, "taxonomicallyKosher" -> tk) ++ failed.filterNot(_._1 == ".qa").map {
      case (key, value) => {
        (key, Json.toJSON(value.toArray))
      }
    } ++ empty
    //revise the properties in the db
    Config.persistenceManager.put(guid, "occ", map, false)

    //check to see if there is a tool QA and remove one
    val dupQA = list.filter(_.code == AssertionCodes.INFERRED_DUPLICATE_RECORD.code)
    //dupQA.foreach(qa => println(qa.getComment))
    if (dupQA.size > 1) {
      val newList: List[QualityAssertion] = list.diff(dupQA) ++ List(dupQA(0))
      //println("Original size " + list.length + "  new size =" + newList.length)
      Config.persistenceManager.putList(guid, "occ", FullRecordMapper.qualityAssertionColumn, newList, classOf[QualityAssertion], true, false)
    }

    (gk, tk)
  }
}

/**
 * A one off class used to repair datum properties.
  *
  * @param centralCounter
 * @param threadId
 * @param startKey
 * @param endKey
 */
class DatumRecordsRunner(centralCounter: Counter, threadId: Int, startKey: String, endKey: String) extends Runnable {
  val logger = LoggerFactory.getLogger("DatumRecordsRunner")
  val processor = new RecordProcessor
  var ids = 0
  val threads = 2
  var batches = 0

  def run {
    val pageSize = 1000
    var counter = 0
    var numIssue = 0
    val start = System.currentTimeMillis
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis

    logger.info("Starting thread " + threadId + " from " + startKey + " to " + endKey)
    def locProcess = new LocationProcessor
    Config.persistenceManager.pageOverSelect("occ", (guid, map) => {
      counter += 1

      if (StringUtils.isNotBlank(map.getOrElse("geodeticDatum", ""))) {
        //check the precision of the lat/lon
        def lat = map.getOrElse("decimalLatitude", "0")
        def lon = map.getOrElse("decimalLongitude", "0")
        def locqa = Json.toIntArray(map.getOrElse("loc.qa", "[]"))
        if (locProcess.getNumberOfDecimalPlacesInDouble(lat) != locProcess.getNumberOfDecimalPlacesInDouble(lon) && locqa.contains(45)) {
          numIssue += 1
          logger.info("FIXME from THREAD " + threadId + "\t" + guid)
        }
      }

      if (counter % pageSize == 0 && counter > 0) {
        centralCounter.addToCounter(pageSize)
        finishTime = System.currentTimeMillis
        centralCounter.printOutStatus(threadId, guid, "Datum")
        startTime = System.currentTimeMillis
      }
      true
    }, startKey, endKey, 1000, "decimalLatitude", "decimalLongitude", "rowKey", "uuid", "geodeticDatum", "loc.qa")
    val fin = System.currentTimeMillis
    logger.info("[Datum Thread " + threadId + "] " + counter + " took " + ((fin - start).toFloat) / 1000f + " seconds")
    logger.info("Finished.")
  }
}

///**
//  * A class that can be used to reload sampling values for all records.
//  *
//  * @param centralCounter
//  * @param threadId
//  * @param startKey
//  * @param endKey
//  */
//class AvroExportRunner(centralCounter: Counter, threadId: Int, startKey: String, endKey: String) extends Runnable {
//
//  val logger = LoggerFactory.getLogger("AvroExportRunner")
//
//  val outputDirPath = "/data/avro-export/shard-" + threadId
//
//  val outputDir = new File(outputDirPath)
//
//  FileUtils.forceMkdir(outputDir)
//
//  val schema = AvroUtil.getAvroSchemaForIndex
//  val writer = AvroUtil.getAvroWriter(outputDirPath + "/records.avro")
//  val indexDAO = new SolrIndexDAO("","","")
//
//  def run {
//
//    var counter = 0
//
//    val start = System.currentTimeMillis
//    logger.info(s"Starting thread $threadId from $startKey to  $endKey")
//    Config.persistenceManager.pageOverAll("occ", (guid, map) => {
//      try {
//        val doc = indexDAO.generateSolrDocument(guid, map, List(), threadId.toString)
//        if(doc != null){
//          val record = new GenericData.Record(schema)
//          AvroUtil.csvHeader.foreach { field =>
//
//            if (indexDAO.multiValueFields.contains(field)) {
//              //add a multi valued field
//              val fieldValues = doc.getFieldValues(field)
//              if(fieldValues != null && !fieldValues.isEmpty){
//                val list = new util.ArrayList[String]
//                val iter = fieldValues.iterator()
//                while (iter.hasNext){
//                  list.add(iter.next().toString)
//                }
//                record.put(field, list)
//              }
//            } else {
//              val fieldValue = doc.getFieldValue(field)
//              if(fieldValue != null && StringUtils.isNotBlank(fieldValue.toString)){
//                record.put(field, fieldValue.toString)
//              }
//            }
//          }
//          if(record.get("id") != null){
//            writer.append(record)
//          }
//        }
//      } catch {
//        case e:Exception => logger.error(s"Problem indexing record: $guid" +" - error message: " + e.getMessage)
//      }
//
//      counter += 1
//      if(counter % 10000 == 0){
//        writer.flush()
//        logger.info(s"[AvroExportRunner Thread $threadId] Export of data $counter, last key $guid")
//      }
//      true
//    }, startKey, endKey, 1000)
//
//    writer.flush()
//    writer.close()
//    val fin = System.currentTimeMillis
//    val timeTakenInSecs = ((fin - start).toFloat) / 1000f
//    logger.info(s"[AvroExportRunner Thread $threadId] $counter took $timeTakenInSecs seconds")
//  }
//}
//
//
//
/**
 * A class that can be used to reload sampling values for all records.
 *
 * @param centralCounter
 * @param threadId
 * @param startKey
 * @param endKey
 */
class LoadSamplingRunner(centralCounter: Counter, threadId: Int, startKey: String, endKey: String) extends Runnable {

  val logger = LoggerFactory.getLogger("LoadSamplingRunner")
  var ids = 0
  val threads = 2
  var batches = 0

  def run {
    var counter = 0
    val start = System.currentTimeMillis
    logger.info("Starting thread " + threadId + " from " + startKey + " to " + endKey)
    Config.persistenceManager.pageOverSelect("occ", (guid, map) => {
      val lat = map.getOrElse("decimalLatitude.p","")
      val lon = map.getOrElse("decimalLongitude.p","")
      if(lat != null && lon != null){
        val point = LocationDAO.getSamplesForLatLon(lat, lon)
        if(!point.isEmpty){
          val (location, environmentalLayers, contextualLayers) = point.get
          Config.persistenceManager.put(guid, "occ", Map(
                      "el.p" -> Json.toJSON(environmentalLayers),
                      "cl.p" -> Json.toJSON(contextualLayers)), false)
        }
        counter += 1
        if(counter % 10000 == 0){
          logger.info("[LoadSamplingRunner Thread " + threadId + "] Import of sample data " + counter + " Last key " + guid)
        }
      }
      true
    }, startKey, endKey, 1000, "decimalLatitude.p", "decimalLongitude.p" )
    val fin = System.currentTimeMillis
    logger.info("[LoadSamplingRunner Thread " + threadId + "] " + counter + " took " + ((fin - start).toFloat) / 1000f + " seconds")
    logger.info("Finished.")
  }
}

/**
 * A class that can be used to reprocess all records in a threaded manner.
 *
 * @param centralCounter
 * @param threadId
 * @param startKey
 * @param endKey
 */
class ProcessRecordsRunner(centralCounter: Counter, threadId: Int, startKey: String, endKey: String) extends Runnable {
  val logger = LoggerFactory.getLogger("ProcessRecordsRunner")
  val processor = new RecordProcessor
  var ids = 0
  val threads = 2
  var batches = 0

  def run {
    val pageSize = 1000
    var counter = 0
    val start = System.currentTimeMillis
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis
    //var buff = new ArrayBuffer[(FullRecord,FullRecord)]
    println("Starting thread " + threadId + " from " + startKey + " to " + endKey)
    val batches = new scala.collection.mutable.ListBuffer[Map[String, Object]]
    val batchSize = 200
    Config.occurrenceDAO.pageOverRawProcessed(rawAndProcessed => {
      counter += 1
      if (!rawAndProcessed.get._1.deleted) {
        if (batches.length == batchSize) {
          processor.writeProcessBatch(batches.toList)
          batches.clear()
        }
        batches += processor.processRecord(rawAndProcessed.get._1, rawAndProcessed.get._2, true)
      }
      if (counter % pageSize == 0 && counter > 0) {
        centralCounter.addToCounter(pageSize)
        finishTime = System.currentTimeMillis
        centralCounter.printOutStatus(threadId, rawAndProcessed.get._1.rowKey, "Processor")
        startTime = System.currentTimeMillis
      }
      true
    }, startKey, endKey, 1000)
    if (batches.length > 0) {
      processor.writeProcessBatch(batches.toList)
    }
    val fin = System.currentTimeMillis
    logger.info("[Processor Thread " + threadId + "] " + counter + " took " + ((fin - start).toFloat) / 1000f + " seconds")
    logger.info("Finished.")
  }
}

/**
 * A runnable thread for creating a complete new index.
 *
 * @param centralCounter
 * @param threadId
 * @param startKey
 * @param endKey
 * @param sourceConfDirPath e.g. solr-template/biocache/conf
 * @param targetConfDirPath e.g. solr-create/biocache-thread-0/conf
 * @param pageSize
 */
class IndexRunner(centralCounter: Counter, threadId: Int, startKey: String, endKey: String,
                  sourceConfDirPath: String, targetConfDirPath: String, pageSize: Int = 200) extends Runnable {

  val logger = LoggerFactory.getLogger("IndexRunner")

  def run {

    //solr-create/thread-0/conf
    val newIndexDir = new File(targetConfDirPath)
    if (newIndexDir.exists) {
      FileUtils.deleteDirectory(newIndexDir)
    }
    FileUtils.forceMkdir(newIndexDir)

    //CREATE a copy of SOLR home
    val sourceConfDir = new File(sourceConfDirPath)   //solr-template/biocache/conf
    FileUtils.copyDirectory(sourceConfDir, newIndexDir)

    //COPY solr-template/biocache/solr.xml  -> solr-create/biocache-thread-0/solr.xml
    FileUtils.copyFileToDirectory(new File(sourceConfDir.getParent + "/solr.xml"), newIndexDir.getParentFile)

    logger.info("Set SOLR Home: " + newIndexDir.getParent)
    val indexer = new SolrIndexDAO(newIndexDir.getParent, Config.excludeSensitiveValuesFor, Config.extraMiscFields)

    // Specify the SOLR config to use
    indexer.solrConfigPath = newIndexDir.getAbsolutePath + "/solrconfig.xml"

    var counter = 0
    val start = System.currentTimeMillis
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis
    var check = true
    val csvFileWriter = if (Config.exportIndexAsCsvPath.length > 0) { indexer.getCsvWriter() } else { null }
    val csvFileWriterSensitive = if (Config.exportIndexAsCsvPath.length > 0) { indexer.getCsvWriter(true) } else { null }

    //page through and create and index for this range
    Config.persistenceManager.pageOverAll("occ", (guid, map) => {
      counter += 1
      val commit = counter % 10000 == 0
      //ignore the record if it has the guid that is the startKey this is because it will be indexed last by the previous thread.
      try {
        if (check) {
          check = false
          //dont index the start key - ranges will exclude the start key but include the endKey
          if (!guid.equals(startKey)) {
            indexer.indexFromMap(guid, map, commit = commit, batchID = threadId.toString, csvFileWriter = csvFileWriter, csvFileWriterSensitive = csvFileWriterSensitive)
          }
        } else {
          indexer.indexFromMap(guid, map, commit = commit, batchID = threadId.toString, csvFileWriter = csvFileWriter, csvFileWriterSensitive = csvFileWriterSensitive)
        }
      } catch {
        case e:Exception => {
          logger.error("Problem indexing record: " + guid + " "  + e.getMessage())
          if(logger.isDebugEnabled){
            logger.debug("Error during indexing: " + e.getMessage, e)
          }
        }
      }

      if (counter % pageSize == 0 && counter > 0) {
        centralCounter.addToCounter(pageSize)
        finishTime = System.currentTimeMillis
        centralCounter.printOutStatus(threadId, guid, "Indexer")
        startTime = System.currentTimeMillis
      }

      true
    }, startKey, endKey, pageSize = pageSize)

    indexer.finaliseIndex(true, true)

    if (csvFileWriter != null) { csvFileWriter.flush(); csvFileWriter.close() }
    if (csvFileWriterSensitive != null) { csvFileWriterSensitive.flush(); csvFileWriterSensitive.close() }

    finishTime = System.currentTimeMillis
    logger.info("Total indexing time for this thread " + ((finishTime - start).toFloat) / 60000f + " minutes.")
  }
}
