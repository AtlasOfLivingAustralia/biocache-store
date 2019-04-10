package au.org.ala.biocache.outliers

import java.io.{File, FileReader}
import java.util.concurrent.{ArrayBlockingQueue, Executors, TimeUnit}

import au.com.bytecode.opencsv.CSVReader
import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.model.QualityAssertion
import au.org.ala.biocache.util.{FileHelper, OptionParser}
import au.org.ala.biocache.vocab.AssertionCodes
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.jayway.jsonpath.JsonPath
import net.minidev.json.JSONArray
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
  * Runnable tool for testing for outliers and updating the data store.
  * This tool works against a dump file extracted from the database after data has been data has been loaded,
  * processed and sampled. The dump file requires a mandatory set of columns plus columns for any environmental
  * variables to run the algorithm with. The mandatory columns are:
  *
  * taxonConceptID
  * taxonRankID
  * rowkey
  * decimalLatitude
  * decimalLongitude
  *
  * An export can be generated using the ExportLocalNode tool.
  */
object ReverseJacknifeProcessor extends Tool {

  def cmd = "jacknife"

  def desc = "Run jacknife processing"

  import FileHelper._

  val logger = LoggerFactory.getLogger("ReverseJacknifeProcessor")
  val mandatoryHeaders = List("taxonConceptID", "taxonRankID", "rowkey", "decimalLatitude", "decimalLongitude")

  def main(args: Array[String]) {

    var fullDumpFilePath: String = ""
    val headerForDumpFile: List[String] = mandatoryHeaders ++ Config.outlierLayerIDs
    var persistResults = true
    var taxonRankThreshold = 7000
    var threads = 1
    var dumpFileSeparator = ','
    var dumpFileQuoteChar = '"'

    val parser = new OptionParser(help) {
      arg("full-dump-file", "Filepath to full extract of data. This is a dump file with the following columns: " +
        headerForDumpFile.mkString(",") +". This can be generated using an sorted export from the database once the " +
        "data has been loaded, processed and sampled.",
        { v: String => fullDumpFilePath = v }
      )
      intOpt("taxonRankThreshold", "The minimum taxon rank threshold to use. The default is 7000 = species.", { v: Int => taxonRankThreshold = v })
      intOpt("t", "threads", "The minimum taxon rank threshold to use. The default is 7000 = species.", { v: Int => threads = v })
      opt("test", "Run jacknife but dont persist results  to the database", { persistResults = false })
      opt("dump-file-separator-char", "Field delimiter used in dump file", { v:String => dumpFileSeparator = v.charAt(0) })
      opt("dump-file-quote-char", "Quote character used in dump file", { v:String => dumpFileQuoteChar = v.charAt(0) })
    }

    if (parser.parse(args)) {
      if(fullDumpFilePath == ""){
        parser.showUsage
      } else if(!new File(fullDumpFilePath).exists()){
        logger.error("Dump file " + fullDumpFilePath + " not available.")
        parser.showUsage
      } else {
        logger.info(s"Using file $fullDumpFilePath, Expecting headers: " + headerForDumpFile.mkString(","))
        runOutlierTestingForDumpFile(fullDumpFilePath, headerForDumpFile, persistResults, taxonRankThreshold, threads, dumpFileSeparator, dumpFileQuoteChar)
        logger.info("Finished.")
        Config.persistenceManager.shutdown
      }
    }
  }

  /**
    * Run outlier detection against the supplied dump file.
    *
    * @param dumpFilePath
    * @param columnHeaders
    */
  def runOutlierTestingForDumpFile(dumpFilePath: String,
                                   columnHeaders: List[String] = List(),
                                   persistResults: Boolean = false,
                                   rankThreshold:Int,
                                   threads:Int,
                                   dumpFileSeparator:Char,
                                   dumpFileQuoteChar:Char
                                  ) {

    val reader: CSVReader = new CSVReader(new FileReader(dumpFilePath), dumpFileSeparator, dumpFileQuoteChar)

    val headers = if (columnHeaders.isEmpty) reader.readNext.toList else columnHeaders

    if (!mandatoryHeaders.forall(headers.contains(_))) {
      throw new RuntimeException("Missing mandatory headers "
        + mandatoryHeaders.mkString(",")
        + ", Got: "
        + headers.mkString(",")
      )
    }

    //get a list of variables
    val variables = headers.filter {
      _.startsWith("el")
    }

    //iterate through file
    var finished = false
    var nextTaxonConceptID = ""
    var nextTaxonRankID = 0
    val timings = new Timings
    var lastLine = Array[String]()

    //the index of decimalLat and long will not change
    val latitudeIdx = headers.indexOf("decimalLatitude")
    val longitudeIdx = headers.indexOf("decimalLongitude")
    val taxonIDIdx = headers.indexOf("taxonConceptID")
    val taxonRankIDIdx = headers.indexOf("taxonRankID")
    val uuidIdx = headers.indexOf("rowkey")
    val rowKeyIdx = headers.indexOf("rowkey")

    logger.info(s"latitudeIdx: $latitudeIdx, longitudeIdx: $longitudeIdx, taxonIDIdx: $taxonIDIdx, taxonRankIDIdx: $taxonRankIDIdx, uuidIdx: $uuidIdx, rowKeyIdx: $rowKeyIdx")

    val queue = new ArrayBlockingQueue[(String, Int, Seq[Array[String]])](30)

    //start consumers
    val executorService = Executors.newFixedThreadPool(threads)
    (0 to 4).foreach { idx =>
      executorService.submit(new Runnable {
        override def run(): Unit = {
          while (!finished || queue.size() > 0) {
            if (queue.size() > 0) {
              val (taxonConceptID, taxonRankID, lines) = queue.take()
              processDataForTaxon(headers,
                persistResults,
                rankThreshold,
                variables,
                timings,
                latitudeIdx,
                longitudeIdx,
                uuidIdx,
                rowKeyIdx,
                taxonConceptID,
                taxonRankID,
                lines
              )
            } else {
              Thread.sleep(1000)
            }
          }
        }
      })
    }

    while (!finished) {

      //read all the data for a single taxon
      val (taxonConceptID, taxonRankID, lines, nextLine) = readAllForTaxon(
        reader,
        nextTaxonConceptID,
        taxonIDIdx,
        nextTaxonRankID,
        taxonRankIDIdx,
        lastLine
      )

      //add to queue for processing
      queue.put((taxonConceptID, taxonRankID, lines))

      if (nextLine == null) {
        finished = true
      } else {
        lastLine = nextLine
        nextTaxonConceptID = nextLine(taxonIDIdx)
        nextTaxonRankID = if(nextLine(taxonRankIDIdx) != ""){
          nextLine(taxonRankIDIdx).toInt
        } else {
          0
        }
      }
    }

    executorService.shutdown()
    executorService.awaitTermination(5, TimeUnit.MINUTES)

    logger.info("Finished paging.")
  }

  private def processDataForTaxon(headers:Seq[String], persistResults: Boolean, rankThreshold: Int, variables: List[String], timings: Timings, latitudeIdx: Int, longitudeIdx: Int, uuidIdx: Int, rowKeyIdx: Int, taxonConceptID: String, taxonRankID: Int, lines: Seq[Array[String]]) = {
    if (StringUtils.isNotBlank(taxonConceptID) && taxonRankID >= rankThreshold) {

      logger.info(s"TaxonID: $taxonConceptID, RankID: $taxonRankID, Records: " + lines.size)
      logger.debug(lines.head.mkString(","))

      val resultsBuffer = new ListBuffer[(String, Seq[SampledRecord], JackKnifeStats)]
      val recordIds = new ListBuffer[String]
      var loadedIds = false

      //run jacknife for each variable
      variables.foreach { variable =>

        //get the column Idx for variable
        val variableIdx = headers.indexOf(variable)
        val pointBuffer = new ListBuffer[SampledRecord]

        //create a set of points
        lines.foreach { line =>
          val variableValue = line(variableIdx)
          val latitude = line(latitudeIdx)
          val longitude = line(longitudeIdx)
          if (!loadedIds) {
            recordIds += line(rowKeyIdx)
          }
          if (variableValue != "" && variableValue != null && latitude != "" && longitude != "") {
            val cellId = latitude.toFloat + "|" + longitude.toFloat
            pointBuffer += SampledRecord(line(uuidIdx), variableValue.toFloat, cellId, Some(line(rowKeyIdx)))
          }
        }

        //we got the points - lets run it
        val (recordsIDs, stats) = performJacknife(pointBuffer)
        if (!stats.isEmpty) {
          resultsBuffer += ((variable, recordsIDs, stats.get))
        }

        //println("Time taken for [Jacknife]: " + (now - startTime)/1000)
        timings checkpoint "jacknife with " + variable

        if (logger.isDebugEnabled()) {
          logger.debug(">>> For layer: %s we've detected: %d outliers out of %d records tested.".format(variable, recordsIDs.length, lines.size))
        }

        if (recordsIDs.length > lines.size) {
          if (logger.isDebugEnabled()) {
            logger.debug(">>> records: %d, distinct values: %d".format(recordsIDs.length, recordsIDs.toSet.size))
          }
          throw new RuntimeException("Error in processing")
        }
        loadedIds = true
      }

      //now identify the records that were not outlier but were tested.
      //If outliers were NOT tested the resultsBuffer will be empty.
      val passedRecords = {
        if (!resultsBuffer.isEmpty) {
          recordIds.toSet diff resultsBuffer.map {
            _._2
          }.foldLeft(ListBuffer[SampledRecord]())(_ ++ _).map(v => v.rowKey.getOrElse(v.id)).toSet
        } else {
          Set[String]()
        }
      }

      logger.debug("The records that passed: " + passedRecords.size + ", total: " + recordIds.size)

      if (persistResults) {
        //store the results for this taxon
        storeResultsWithStats(taxonConceptID, resultsBuffer, passedRecords, "species_guid")
      }
    }
  }

  /**
    * Reads all the records for the supplied taxonConceptID
    *
    * @param reader
    * @param taxonConceptID
    * @return
    */
  def readAllForTaxon(reader: CSVReader,
                      taxonConceptID: String,
                      taxonConceptIDIdx: Int,
                      taxonRankID: Int,
                      taxonRankIDIdx: Int,
                      lastLine: Array[String] = Array()): (String, Int, Seq[Array[String]], Array[String]) = {

    var currentLine: Array[String] = reader.readNext()
    val idForBatch: String = if (taxonConceptID != "") {
      taxonConceptID
    } else {
      currentLine(taxonConceptIDIdx)
    }

    val taxonRankID = currentLine(taxonRankIDIdx)

    val rankIdForBatch: Int = if(StringUtils.isNotBlank(taxonRankID)){
      taxonRankID.toInt
    } else {
      0
    }

    logger.debug("###### Running for:" + idForBatch)

    val buffer = new ListBuffer[Array[String]]
    if (!lastLine.isEmpty) {
      buffer += lastLine
    }

    while (currentLine != null && currentLine(taxonConceptIDIdx) == idForBatch) {
      buffer += currentLine
      currentLine = reader.readNext()
    }

    (idForBatch, rankIdForBatch, buffer, currentLine)
  }

  /**
    * Persist the results of jacknife run the underlying database for the system (Cassandra).
    *
    * @param taxonID
    * @param results
    * @param passed
    * @param field
    */
  def storeResultsWithStats(taxonID: String, results: Seq[(String, Seq[SampledRecord], JackKnifeStats)],
                            passed: Set[String],  field: String) {

    val mapper = new ObjectMapper
    mapper.registerModule(DefaultScalaModule)

    val jackKnifeStatsMap = results.map(x => x._1 -> x._3).toMap[String, JackKnifeStats]

    //store the outlier stats for this taxon
    Config.persistenceManager.put(taxonID, "outliers", "jackKnifeStats", mapper.writeValueAsString(jackKnifeStatsMap), true, false)

    //recordUUID -> list of layers
    val variableResults = results.map(x => (x._1, x._2))
    val record2OutlierLayer: Seq[(SampledRecord, String)] = invertLayer2Record(variableResults)

    val previous = try {
      Config.persistenceManager.get(taxonID, "outliers", "jackKnifeOutliers")
    } catch {
      case _: Exception => None
    }

    if (!previous.isEmpty) {
      Config.persistenceManager.put(taxonID, "outliers", "previous", previous.get, true, false)
    }

    //mark up records
    Config.persistenceManager.put(taxonID, "outliers", "jackKnifeOutliers", mapper.writeValueAsString(variableResults), true, false)

    val recordStats = record2OutlierLayer.map(x => {
      val (sampledRecord, layerId) = (x._1, x._2)
      //lookup stats for this record
      val jackKnifeStats = jackKnifeStatsMap.get(layerId).get
      //need to get the value for this record
      RecordJackKnifeStats(sampledRecord.id,
        layerId,
        sampledRecord.value,
        jackKnifeStats.sampleSize,
        jackKnifeStats.min,
        jackKnifeStats.max,
        jackKnifeStats.mean,
        jackKnifeStats.stdDev,
        jackKnifeStats.range,
        jackKnifeStats.threshold,
        jackKnifeStats.outlierValues)
    })

    recordStats.groupBy(_.uuid).foreach { x =>
      val rowKey = x._1
      if (!rowKey.isEmpty) {
        val layerIds = x._2.map(_.layerId).toList
        Config.persistenceManager.put(rowKey, "occ", "outlierForLayers" + Config.persistenceManager.fieldDelimiter + "p", mapper.writeValueAsString(layerIds), true, false)
        Config.occurrenceDAO.addSystemAssertion(rowKey, QualityAssertion(AssertionCodes.DETECTED_OUTLIER, "Outlier for " + x._2.size + " layers"))
        Config.persistenceManager.put(x._1, "occ_outliers", "jackKnife", mapper.writeValueAsString(x._2), true, false)
      } else {
        logger.debug("Row key lookup failed for : " + x._1)
      }
    }

    //reset the records that are no longer considered outliers -
    //do an ID diff between the results
    val previousIDs: Array[String] = {
      if (previous.isDefined) {
        val jsonPath = JsonPath.compile("$..id")
        val idArray = jsonPath.read(previous.get).asInstanceOf[JSONArray]
        idArray.toArray(Array[String]())
      } else {
        Array[String]()
      }
    }

    logger.debug("previousIDs: " + previousIDs)
    val currentIDs = record2OutlierLayer.map({ case (sampledRecord, id) => sampledRecord.id }).toArray

    //IDs in the previous not in current need to be reset
    val newIDs = previousIDs diff currentIDs
    logger.debug("previous : " + previousIDs.size + " current " + currentIDs.size + " new : " + newIDs.size)
    logger.debug("[WARNING] Number of old IDs not marked as outliers anymore: " + newIDs.size)

    newIDs.foreach { recordID =>
      logger.debug("Record " + recordID + " is no longer an outlier")
      val rowKey = Config.occurrenceDAO.getRowKeyFromUuid(recordID)
      if (!rowKey.isEmpty) {
        //add the uuid as one that is needing reindexing:
        Config.persistenceManager.deleteColumns(rowKey.get, "occ", "outlierForLayers" + Config.persistenceManager.fieldDelimiter + "p")
        //remove the system assertions
        //Config.occurrenceDAO.removeSystemAssertion(rowKey.get, AssertionCodes.DETECTED_OUTLIER)
        Config.occurrenceDAO.addSystemAssertion(rowKey.get, QualityAssertion(AssertionCodes.DETECTED_OUTLIER, 1), replaceExistCode = true)
      }
    }
  }

  def invertLayer2Record(layer2Record: Seq[(String, Seq[SampledRecord])]): Seq[(SampledRecord, String)] = {
    //group by recordUuid
    val record2Layer = new ListBuffer[(SampledRecord, String)]

    layer2Record.foreach { layer2Record =>
      val (layerId, records) = layer2Record
      records.foreach { r => record2Layer.append((r, layerId)) }
    }

    record2Layer
  }

  /**
    * Run jacknife on these points, returning the IDs of the records that are outliers.
    *
    * @param points
    * @return a list of UUIDs for records that are outliers
    */
  def performJacknife(points: Seq[SampledRecord]): (Seq[SampledRecord], Option[JackKnifeStats]) = {

    //we have a points, group by on cellId as we only want to sample once per cell
    val pointsGroupedByCell = points.groupBy(p => p.cellId)

    //create a cell -> value map
    val cellToValue = pointsGroupedByCell.map(g => g._1 -> g._2.head.value).toMap[String, Float]

    //create a value -> cell map    
    val valuesToCells = cellToValue.groupBy(x => x._2).map(x => x._1 -> x._2.keys.toSet[String]).toMap

    //the environmental properties to throw at Jack knife test
    val valuesToTest = cellToValue.values.map(y => y.toFloat).toSeq

    //do jack knife test
    JackKnife.jackknife(valuesToTest) match {
      case Some(stats) => {
        val outliers = new ListBuffer[SampledRecord]
        stats.outlierValues.foreach { x =>
          //get the cell
          val cellIds = valuesToCells.getOrElse(x, Set())
          cellIds.foreach { cellId =>
            val points = pointsGroupedByCell.get(cellId).get
            points.foreach(point => outliers += point)
          }
        }
        (outliers.distinct, Some(stats))
      }
      case None => (List(), None)
    }
  }
}

class Timings {
  val logger = LoggerFactory.getLogger("Timings")

  val startTime = System.currentTimeMillis()
  var lapTime = startTime

  def checkpoint(comment: String) {
    val now = System.currentTimeMillis()
    logger.debug("Time taken for [%s] : %f seconds.".format(comment, ((now - lapTime).toFloat / 1000.0f)))
    lapTime = now
  }
}