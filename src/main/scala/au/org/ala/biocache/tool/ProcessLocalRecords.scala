package au.org.ala.biocache.tool

import java.io.File
import java.util.Date

import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.model.Versions
import au.org.ala.biocache.processor.RecordProcessor
import au.org.ala.biocache.util.{OptionParser, ZookeeperUtil}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * A tool for processing local records.
  */
object ProcessLocalRecords extends Tool {

  def cmd = "process-local-node"

  def desc = "Process all records on a local database node"

  def main(args: Array[String]) {

    var threads: Int = 1
    var drs: Seq[String] = List()
    var skipDrs: Seq[String] = List()
    var useFullScan = false
    var startTokenRangeIdx = 0
    var taxaFile = ""
    val checkpointFile = Config.tmpWorkDir + "/process-local-records-checkpoints.txt"

    val parser = new OptionParser(help) {
      intOpt("t", "thread", "The number of threads to use", { v: Int => threads = v })
      opt("dr", "data-resource-list", "comma separated list of drs to process", {
        v: String =>
          drs = v.trim.split(",").map {
            _.trim
          }
      })
      opt("edr", "skip-data-resource-list", "comma separated list of drs to NOT process", {
        v: String => {
          skipDrs = v.trim.split(",").map {
            _.trim
          }
          useFullScan = true
        }
      })
      opt("tl", "taxa-list", "file containing a list of taxa to reprocess", {
        v: String => taxaFile = v
      })
      opt("use-full-scan", "Use a full table scan. This is faster if most of the data needs to be processed", {
        useFullScan = true
      })
      opt("use-resource-scan", "Scan by data resource. This is slower for very large datasets (> 5 million record), but faster for smaller", {
        useFullScan = false
      })
      intOpt("stk", "start-token-range-idx", "the idx of the token range to start at. Typically a value between 0 and 1024." +
        "This is useful when a long running process fails for some reason.", {
        v: Int => startTokenRangeIdx = v
      })
    }

    if (skipDrs.isEmpty && drs.size == 1) {
      useFullScan = false
    }

    if (drs.isEmpty || !skipDrs.isEmpty) {
      useFullScan = true
    }

    if (parser.parse(args)) {
      if (taxaFile != "") {
        new ProcessLocalRecords().processTaxaOnly(threads, taxaFile, startTokenRangeIdx, checkpointFile)
      } else {
        new ProcessLocalRecords().processRecords(threads, drs, skipDrs, useFullScan, startTokenRangeIdx, checkpointFile)
      }
    }
  }
}

/**
  * A class for processing local records for this node.
  */
class ProcessLocalRecords {

  val logger = LoggerFactory.getLogger("ProcessLocalRecords")

  /**
    * Reads the taxonIDs supplied in the file and only processes records that match this list.
    *
    * @param threads
    * @param taxaFilePath
    * @param startTokenRangeIdx
    * @param checkpointFile
    */
  def processTaxaOnly(threads: Int, taxaFilePath: String, startTokenRangeIdx: Int, checkpointFile: String): Unit = {

    ZookeeperUtil.setStatus("PROCESSING", "STARTING", 0)
    //read the taxa file
    val taxaIDList = Source.fromFile(new File(taxaFilePath)).getLines().toSet[String]
    logger.info("Number of taxa to process " + taxaIDList.size)

    val processor = new RecordProcessor
    var count = 0
    var matchedCount = 0
    var lastMatched = ""

    setCheckpoints(startTokenRangeIdx, checkpointFile)

    Config.persistenceManager.pageOverLocal("occ", (rowkey, map, batchID) => {
      val taxonConceptID = map.getOrElse("taxonConceptID" + Config.persistenceManager.fieldDelimiter + "p", "")
      if (taxonConceptID != "" && taxaIDList.contains(taxonConceptID)) {
        val records = Config.occurrenceDAO.getAllVersionsByRowKey(rowkey)
        if (!records.isEmpty) {
          processor.processRecord(records.get(0), records.get(1))
          synchronized {
            matchedCount += 1
            lastMatched = rowkey
          }
        }
      }
      count += 1
      if (count % 100000 == 0) {
        logger.info(s"Total read : $count, total matched: $matchedCount, last matched $lastMatched")
        ZookeeperUtil.setStatus("PROCESSING", "RUNNING", count)
      }

      true
    },
      threads,
      Array("rowkey", "taxonConceptID" + Config.persistenceManager.fieldDelimiter + "p")
    )

    //Move checkpoint file if complete
    new File(checkpointFile).renameTo(new File(checkpointFile + ".complete"))
    ZookeeperUtil.setStatus("PROCESSING", "COMPLETE", count)
    logger.info("Finished reprocessing. Total matched and reprocessed: " + count)
  }

  def processRecords(threads: Int, drs: Seq[String], skipDrs: Seq[String], useFullScan: Boolean,
                     startTokenRangeIdx: Int, checkpointFile: String): Unit = {

    val processor = new RecordProcessor
    val start = System.currentTimeMillis()
    var lastLog = System.currentTimeMillis()

    //note this update count isn't thread safe, so its inaccurate
    //its been left in to give a general idea of performance
    var updateCount = 0
    var readCount = 0

    setCheckpoints(startTokenRangeIdx, checkpointFile)

    val total = if (useFullScan) {
      logger.info("Using a full scan...")
      Config.occurrenceDAO.pageOverRawProcessedLocal(record => {
        if (!record.isEmpty) {
          val raw = record.get._1
          val rowKey = raw.rowKey
          readCount += 1
          if ((drs.isEmpty || drs.contains(raw.attribution.dataResourceUid)) &&
            !skipDrs.contains(raw.attribution.dataResourceUid)) {
            val (processed, assertions) = processor.processRecord(raw)
            //set the last processed time
            processed.lastModifiedTime = org.apache.commons.lang.time.DateFormatUtils.format(
              new Date, "yyyy-MM-dd'T'HH:mm:ss'Z'")
            Config.occurrenceDAO.updateOccurrence(rowKey, processed, Some(assertions), Versions.PROCESSED)
            updateCount += 1
          }
          if (updateCount % 10000 == 0) {

            val end = System.currentTimeMillis()
            val timeInSecs = ((end - lastLog).toFloat / 10000f)
            val recordsPerSec = Math.round(10000f / timeInSecs)
            logger.info(s"Total processed : $updateCount, total read: $readCount Last rowkey: $rowKey  Last 1000 in $timeInSecs seconds ($recordsPerSec records a second)")
            lastLog = end
            ZookeeperUtil.setStatus("PROCESSING", "RUNNING", updateCount)
          }
        }
        true
      }, null, threads)
    } else {
      logger.info("Using a sequential scan by data resource...")
      var total = 0
      drs.foreach { dataResourceUid =>
        logger.info(s"Using a sequential scan by data resource...starting $dataResourceUid")
        total = total + Config.occurrenceDAO.pageOverRawProcessedLocal(record => {
          if (!record.isEmpty) {
            val raw = record.get._1
            val rowKey = raw.rowKey
            updateCount += 1
            val (processed, assertions) = processor.processRecord(raw)
            processed.lastModifiedTime = org.apache.commons.lang.time.DateFormatUtils.format(
              new Date, "yyyy-MM-dd'T'HH:mm:ss'Z'")
            Config.occurrenceDAO.updateOccurrence(rowKey, processed, Some(assertions), Versions.PROCESSED)

            if (updateCount % 100 == 0 && updateCount > 0) {
              val end = System.currentTimeMillis()
              val timeInSecs = ((end - lastLog).toFloat / updateCount.toFloat)
              val recordsPerSec = Math.round(updateCount.toFloat / timeInSecs)
              logger.info(s"Total processed : $updateCount, Last rowkey: $rowKey  Last 1000 in $timeInSecs seconds ($recordsPerSec records a second)")
              lastLog = end
            }
          }
          true
        }, dataResourceUid, threads)
      }
      //sequential scan for each resource
      total
    }

    //Move checkpoint file if complete
    new File(checkpointFile).renameTo(new File(checkpointFile + ".complete"))

    ZookeeperUtil.setStatus("PROCESSING", "COMPLETED", total)
    val end = System.currentTimeMillis()
    val timeInMinutes = ((end - start).toFloat / 100f / 60f / 60f)
    val timeInSecs = ((end - start).toFloat / 1000f)
    logger.info(s"Total records processed : $total in $timeInSecs seconds (or $timeInMinutes minutes)")
  }

  def setCheckpoints(startTokenRangeIdx: Int, checkpointFile: String): Any = {
    System.setProperty("startAtTokenRange", startTokenRangeIdx.toString)
    System.setProperty("tokenRangeCheckPointFile", checkpointFile)

    if (new File(checkpointFile).exists()) {

      logger.info("Checkpoint file detected. Will attempt to restart process.....")

      //completed token ranges
      var completedTokenRanges = ListBuffer[String]()
      Source.fromFile(checkpointFile).getLines().foreach { line =>
        val parts = line.split(",")
        if (parts.length == 2) {
          completedTokenRanges += parts(0)
        }
      }

      System.setProperty("completedTokenRanges", completedTokenRanges.mkString(","))
    }
  }
}
