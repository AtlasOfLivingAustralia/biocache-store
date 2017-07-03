package au.org.ala.biocache.index

import java.io.{File, FileWriter}
import java.net.URL
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong

import au.com.bytecode.opencsv.CSVWriter
import au.org.ala.biocache._
import au.org.ala.biocache.caches.{LocationDAO, TaxonProfileDAO}
import au.org.ala.biocache.index.lucene.LuceneIndexing
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.model.QualityAssertion
import au.org.ala.biocache.processor.{LocationProcessor, Processors, RecordProcessor}
import au.org.ala.biocache.util.Json
import au.org.ala.biocache.vocab.{AssertionCodes, SpeciesGroups}
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashSet, ListBuffer}
import scala.io.Source
import scala.util.parsing.json.JSON

trait Counter {

  val logger = LoggerFactory.getLogger("Counter")

  var counter = 0

  def addToCounter(amount: Int) = counter += amount

  var startTime = System.currentTimeMillis
  var finishTime = System.currentTimeMillis

  def printOutStatus(threadId: Int, lastKey: String, runnerType: String, totalTime: Long = 0) = {
    var average = ""
    if (totalTime > 0) {
      average = "Average record/s: " + (counter / ((System.currentTimeMillis() - totalTime) / 1000f))
    }
    finishTime = System.currentTimeMillis
    logger.info("[" + runnerType + " Thread " + threadId + "] " + counter + " >> " + average + ", Last key : " + lastKey)
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

    val json = try {
      val firstRequest = Config.biocacheServiceUrl + "/occurrences/search?q=" + query + "&pageSize=1&facet=off&sort=row_key&dir=asc"
      JSON.parseFull(Source.fromURL(new URL(firstRequest)).mkString)
    } catch {
      case e:Exception => "Problem retrieving range."
        None
    }

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
class ColumnExporter(centralCounter: Counter, threadId: Int, startKey: String, endKey: String, columns: List[String], includeRowkey:Boolean, separator:Char = '\t', quoteChar:Char = '"', escapeChar:Char = '\\') extends Runnable {

  val logger = LoggerFactory.getLogger("ColumnExporter")

  def run {

    val outWriter = new FileWriter(new File( Config.tmpWorkDir + "/fullexport" + threadId + ".txt"))
    val writer = new CSVWriter(outWriter, separator, quoteChar, escapeChar)
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

class BulkColumnExporter(centralCounter: Counter, threadId: Int, startKey: String, endKey: String,
                         fieldLists: ListBuffer[(Array[String], Array[String], Array[Int], Boolean)],
                         outputStreams: ArrayBuffer[CSVWriter], filters: ListBuffer[(String, String)]) extends Runnable {

  val elpattern = """el[0-9]+""".r
  val clpattern = """cl[0-9]+""".r

  val logger = LoggerFactory.getLogger("BulkColumnExporter")

  def run {
    val start = System.currentTimeMillis
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis
    var counter = 0
    val pageSize = 10000
    Config.persistenceManager.pageOverAll("occ", (key, map) => {
      counter += 1
      try {
        if (!map.getOrElse("uuid", "").isEmpty && map.getOrElse(FullRecordMapper.deletedColumn, "").isEmpty) {
          for (i <- 0 until filters.length) {
            val v = filters(i)
            var found = false

            if (v._1.equals("species_group")) {
              val lft = map.get("left.p")
              val rgt = map.get("right.p")
              if (lft.isDefined && rgt.isDefined) {
                // check the species groups
                val sgs = SpeciesGroups.getSpeciesGroups(lft.get, rgt.get)
                if (sgs.isDefined) {
                  sgs.get.foreach { v1 => if (v._2.equals(v1)) found = true }
                }
              }
            }

            if (found || map.getOrElse(v._1, "").equals(v._2) || ("*".equals(v._2) && !map.getOrElse(v._1, "").isEmpty)) {
              exportRecord(outputStreams(i), fieldLists(i), key, map)
            }
          }
        }
      } catch {
        case e:Exception => logger.error("failed to export: " + key, e);
      }
      if (counter % pageSize == 0 && counter > 0) {
        centralCounter.addToCounter(pageSize)
        finishTime = System.currentTimeMillis
        centralCounter.printOutStatus(threadId, key, "BulkColumnExporter")
        startTime = System.currentTimeMillis
      }
      true
    }, startKey, endKey, 1000)

    val fin = System.currentTimeMillis
    logger.info("[Exporter Thread " + threadId + "] " + counter + " took " + ((fin - start).toFloat) / 1000f + " seconds")
  }

  def exportRecord(writer: CSVWriter, fieldsToExport: (Array[String], Array[String], Array[Int], Boolean),
                   guid: String, map: Map[String, String]) {
    val (fields, layers, qa, userQa) = fieldsToExport
    val values = new ArrayBuffer[String]()

    for (i <- 0 until fields.length) {
      values += map.getOrElse(fields(i), "").replace("\n", "")
    }

    if (layers != null) {
      val ly = Json.toStringMap(map.getOrElse("el.p", "{}")) ++ Json.toStringMap(map.getOrElse("cl.p", "{}"))
      for (i <- 0 until layers.length) {
        values += ly.getOrElse(layers(i), "").replace("\n", "")
      }
    }

    if (qa != null) {
      //now handle the QA fields
      val failedCodes = getErrorCodes(map);
      //work way through the codes and add to output
      for (i <- 0 until qa.length) {
        values += (failedCodes.contains(qa(i))).toString.replace("\n", "")
      }
    }

    if (userQa) {
      if (map.contains(FullRecordMapper.userQualityAssertionColumn))
        values += getUserAssertionsString(map.getOrElse("rowKey","").replace("\n", "")) else ""
    }

    writer.writeNext(values.toArray)
  }

  def getErrorCodes(map:Map[String, String]):Array[Integer]={
    val array:Array[List[Integer]] = FullRecordMapper.qaFields.filter(field => map.get(field).getOrElse("[]") != "[]").toArray.map(field => {
      Json.toListWithGeneric(map.get(field).get,classOf[java.lang.Integer])
    }).asInstanceOf[Array[List[Integer]]]
    if(!array.isEmpty)
      return array.reduceLeft(_++_).toArray
    return Array()
  }

  def getUserAssertionsString(rowKey:String):String ={
    val assertions:List[QualityAssertion] = getUserAssertions(rowKey)
    val string:StringBuilder = new StringBuilder()
    assertions.foreach( assertion => {
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
    })
    string.toString()
  }

  def getUserAssertions(rowKey:String): List[QualityAssertion] ={
    val startKey = rowKey + "|"
    val endKey = startKey + "~"
    val userAssertions = new ArrayBuffer[QualityAssertion]
    //page over all the qa's that are for this record
    Config.persistenceManager.pageOverAll("qa",(guid, map)=>{
      val qa = new QualityAssertion()
      qa.referenceRowKey = guid
      FullRecordMapper.mapPropertiesToObject(qa, map)
      userAssertions += qa
      true
    },startKey, endKey, 1000)

    userAssertions.toList
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
  * A class that can be used to reload taxon conservation values for all records.
  *
  * @param centralCounter
  * @param threadId
  * @param startKey
  * @param endKey
  */
class LoadTaxonConservationData(centralCounter: Counter, threadId: Int, startKey: String, endKey: String) extends Runnable {

  val logger = LoggerFactory.getLogger("LoadTaxonConservationData")
  var ids = 0
  val threads = 2
  var batches = 0

  def run {
    var counter = 0
    val start = System.currentTimeMillis
    logger.info("Starting thread " + threadId + " from " + startKey + " to " + endKey)
    Config.persistenceManager.pageOverSelect("occ", (guid, map) => {
      val updates = mutable.Map[String, String]()
      val taxonProfileWithOption = TaxonProfileDAO.getByGuid(map.getOrElse("taxonConceptID.p", ""))
      if(!taxonProfileWithOption.isEmpty){
        val taxonProfile = taxonProfileWithOption.get
        //add the conservation status if necessary
        if (taxonProfile.conservation != null) {
          val country = taxonProfile.retrieveConservationStatus(map.getOrElse("country.p", ""))
          updates.put("countryConservation.p", country.getOrElse(""))
          val state = taxonProfile.retrieveConservationStatus(map.getOrElse("stateProvince.p", ""))
          updates.put("stateConservation.p", state.getOrElse(""))
          val global = taxonProfile.retrieveConservationStatus("Global")
          updates.put("Global", global.getOrElse(""))
        }
      }
      if (updates.size < 3) {
        updates.put("countryConservation.p", "")
        updates.put("stateConservation.p", "")
        updates.put("Global", "")
      }
      val changes = updates.filter(it => map.getOrElse(it._1, "") != it._2)
      if (!changes.isEmpty) {
        Config.persistenceManager.put(guid, "occ", changes.toMap, true)
      }

      counter += 1
      if(counter % 10000 == 0){
        logger.info("[LoadTaxonConservationData Thread " + threadId + "] Import of sample data " + counter + " Last key " + guid)
      }

      true
    }, startKey, endKey, 1000, "taxonConceptID.p", "country.p", "countryConservation.p", "stateProvince.p", "stateConservation.p", "Global")
    val fin = System.currentTimeMillis
    logger.info("[LoadTaxonConservationData Thread " + threadId + "] " + counter + " took " + ((fin - start).toFloat) / 1000f + " seconds")
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
                  sourceConfDirPath: String, targetConfDirPath: String, pageSize: Int = 200,
                  luceneIndexing: LuceneIndexing = null,
                  processingThreads: Integer = 1,
                  processorBufferSize: Integer = 100,
                  singleWriter: Boolean = false) extends Runnable {

  val logger = LoggerFactory.getLogger("IndexRunner")

  val startTimeFinal = System.currentTimeMillis()

  val directoryList = new java.util.ArrayList[File]

  val timing = new AtomicLong(0)

  def run {

    val useLucene = luceneIndexing != null
    //need to synchronize luceneIndexing for docBuilder.index() when indexer.commitThreadCount == 0
    val lock = if (useLucene && luceneIndexing.getCommitThreadCount == 0) null else new Object()


    //solr-create/thread-0/conf
    val newIndexDir = new File(targetConfDirPath)

    if (!useLucene) {
      if (newIndexDir.exists) {
        FileUtils.deleteDirectory(newIndexDir)
      }
      FileUtils.forceMkdir(newIndexDir)

      //CREATE a copy of SOLR home
      val sourceConfDir = new File(sourceConfDirPath) //solr-template/biocache/conf
      FileUtils.copyDirectory(sourceConfDir, newIndexDir)

      //COPY solr-template/biocache/solr.xml  -> solr-create/biocache-thread-0/solr.xml
      FileUtils.copyFileToDirectory(new File(sourceConfDir.getParent + "/solr.xml"), newIndexDir.getParentFile)

      logger.info("Set SOLR Home: " + newIndexDir.getParent)
    }

    val indexer = new SolrIndexDAO(newIndexDir.getParent, Config.excludeSensitiveValuesFor, Config.extraMiscFields)

    var counter = 0
    val start = System.currentTimeMillis
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis

    val csvFileWriter = if (Config.exportIndexAsCsvPath.length > 0) {
      indexer.getCsvWriter()
    } else {
      null
    }
    val csvFileWriterSensitive = if (Config.exportIndexAsCsvPath.length > 0) {
      indexer.getCsvWriter(true)
    } else {
      null
    }

    val queue: LinkedBlockingQueue[Map[String, String]] = new LinkedBlockingQueue[Map[String, String]](processorBufferSize)

    val threads = mutable.ArrayBuffer[ProcessThread]()
    if (luceneIndexing != null) {
      indexer.luceneIndexing = luceneIndexing
    }

    if (processingThreads > 0 && luceneIndexing != null) {
      for (i <- 0 until processingThreads) {
        var t = new ProcessThread()
        t.start()
        threads += t
      }
    }

    class ProcessThread extends Thread() {
      val docBuilder = luceneIndexing.getDocBuilder
      var recordsProcessed = 0

      override def run() {

        // Specify the SOLR config to use
        indexer.solrConfigPath = newIndexDir.getAbsolutePath + "/solrconfig.xml"
        var continue = true
        while (continue) {
          val m: Map[String, String] = queue.take()
          if (m.isEmpty) {
            continue = false
          } else {
            try {
              if (m != null && m.size > 0) {
                val t1 = System.nanoTime()
                val t2 = indexer.indexFromMapNew(m.get("uuid").get, m, docBuilder = docBuilder, lock = lock)
                timing.addAndGet(System.nanoTime() - t1 - t2)
              }
            } catch {
              case e: InterruptedException => throw e
              case e: Exception => logger.error("guid:" + m.get("uuid") + ", " + e.getMessage())
            }
          }
          recordsProcessed = recordsProcessed + 1
        }
      }
    }

    //page through and create and index for this range
    val t2Total = new AtomicLong(0L)
    var t2 = System.nanoTime()
    Config.persistenceManager.pageOverAll("occ", (guid, map) => {
      t2Total.addAndGet(System.nanoTime() - t2)

      counter += 1
      val commit = counter % 100000 == 0
      //ignore the record if it has the guid that is the startKey this is because it will be indexed last by the previous thread.
      try {
        //dont index the start key - ranges will exclude the start key but include the endKey
        if (!guid.equals(startKey)) {
          if (map.contains("uuid") && !StringUtils.isEmpty(map.getOrElse("uuid", ""))) {
            val t1 = System.nanoTime()
            var t2 = 0L
            if (useLucene) {
              if (processingThreads > 0) queue.put(map)
              else t2 = indexer.indexFromMapNew(map.get("uuid").get, map)
            } else {
              indexer.indexFromMap(guid, map, commit = commit, batchID = threadId.toString, csvFileWriter = csvFileWriter, csvFileWriterSensitive = csvFileWriterSensitive)
            }
            timing.addAndGet(System.nanoTime() - t1 - t2)
          }
        }
      } catch {
        case e: Exception => {
          logger.error("Problem indexing record: " + guid + " " + e.getMessage(), e)
          if (logger.isDebugEnabled) {
            logger.debug("Error during indexing: " + e.getMessage, e)
          }
        }
      }

      if (counter % pageSize * 10 == 0 && counter > 0) {
        centralCounter.addToCounter(pageSize)
        finishTime = System.currentTimeMillis
        centralCounter.printOutStatus(threadId, guid, "Indexer", startTimeFinal)

        if (useLucene) {
          /*
         tuning:

         queues and controlling config:
           for each thread(--thread) there is a stack of queues
             Cassandra(--pagesize) ->
                Processing(solr.batch.size) ->
                    LuceneDocuments(solr.hard.commit.size) ->
                        CommitBatch(solr.hard.commit.size / (writerthreads + 1))

         --processorBufferSize can be small, (100). Ideally it is the same as --pagesize, memory permitting.

         --writterBufferSize should be large (10000) to reduce commit batch overhead.

         --writerram (default=200 MB) is large to reduce the number of disk flushes. The impact can be observed
         comparing 'index docs committed/in ram' values. Note that memory required is --writerram * --threads MB.
         The lesser size of --writeram to exceed is that it is large enough to
         fit --writterBufferSize / (writerthreads + 2) documents. The average doc size
         can be determined from 'in ram/ram MB'. The larger --writerram is, the less merging that will be required
         when finished.

         --pagesize should be adjusted for cassandra read performance (1000)

         --threads can be reduced but this may have the largest impact on performance. Compare 'average records/s'
         for different settings.

         --threads increase should more quickly add documents to the Processing queue, if it is consistently low.
         This has a large impact on memory required.

         --processthreads should be increased if the Processing queue is consistently full. Also depends on available CPU.

         --writerthreads may be increased if the LuceneDocuments queue is consistently full. This has low impact on
         writer performance. Also depends on available CPU.

         --writersegmentsize should not be low because more segments are produced and will need to be merged at the end of
         indexing. If it is too large the performance on producing the lucene index diminish over time, for each --thread.

         To run indexing without the queues Processing, LuceneDocuments, CommitBatch and their associated threads,
         use --processthreads=0 and --writerthreads=0. This is for low mem/slow disk/low number of CPU systems.

         After adjusting the number of threads, the bottleneck; cassandra, processing or lucene, can be observed with
         cassandraTime, processingTime, and solrTime or the corresponding queue sizes.

          */

          logger.info("cassandraTime(s)=" + (t2Total.get()) / 1000000000 +
            ", processingTime[" + processingThreads + "](s)=" + timing.get() / 1000000000 +
            ", solrTime[" + luceneIndexing.getThreadCount() + "](s)=" + luceneIndexing.getTiming() / 1000000000 +
            ", index docs committed/in ram/ram MB=" +
            luceneIndexing.getCount() + "/" + luceneIndexing.ramDocs() + "/" + (luceneIndexing.ramBytes() / 1024 / 1024) +
            ", mem free(Mb)=" + Runtime.getRuntime().freeMemory() / 1024 / 1024 +
            ", mem total(Mb)=" + Runtime.getRuntime().maxMemory() / 1024 / 1024 +
            ", queues (processing/lucene docs/commit batch) " + queue.size() + "/" + luceneIndexing.getQueueSize() + "/" + luceneIndexing.getBatchSize())
        }

        startTime = System.currentTimeMillis
      }

      t2 = System.nanoTime()

      true
    }, startKey, endKey, pageSize = pageSize)

    //final log entry
    centralCounter.printOutStatus(threadId, "", "Indexer", startTimeFinal)

    if (useLucene) {
      logger.info("FINAL >>> cassandraTime(s)=" + (t2Total.get()) / 1000000000 +
        ", processingTime[" + processingThreads + "](s)=" + timing.get() / 1000000000 +
        ", solrTime[" + luceneIndexing.getThreadCount() + "](s)=" + luceneIndexing.getTiming() / 1000000000 +
        ", index docs committed/in ram/ram MB=" +
        luceneIndexing.getCount() + "/" + luceneIndexing.ramDocs() + "/" + (luceneIndexing.ramBytes() / 1024 / 1024) +
        ", mem free(Mb)=" + Runtime.getRuntime().freeMemory() / 1024 / 1024 +
        ", mem total(Mb)=" + Runtime.getRuntime().maxMemory() / 1024 / 1024 +
        ", queues (processing/lucene docs/commit batch) " + queue.size() + "/" + luceneIndexing.getQueueSize() + "/" + luceneIndexing.getBatchSize())
    }

    if (csvFileWriter != null) {
      csvFileWriter.flush(); csvFileWriter.close()
    }
    if (csvFileWriterSensitive != null) {
      csvFileWriterSensitive.flush(); csvFileWriterSensitive.close()
    }

    //signal threads to end
    for (i <- 0 until processingThreads) {
      queue.put(Map[String, String]())
    }

    //wait for threads to end
    threads.foreach(t => t.join())

    finishTime = System.currentTimeMillis
    logger.info("Total indexing time for this thread " + ((finishTime - start).toFloat) / 60000f + " minutes.")

    //close and merge the lucene index parts
    if (luceneIndexing != null && !singleWriter) {
      luceneIndexing.close(true, true)
    }
  }
}
