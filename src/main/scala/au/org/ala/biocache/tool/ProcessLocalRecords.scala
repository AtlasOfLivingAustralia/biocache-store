package au.org.ala.biocache.tool

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import au.org.ala.biocache.Config
import au.org.ala.biocache.caches._
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.persistence.Cassandra3PersistenceManager
import au.org.ala.biocache.processor.RecordProcessor
import au.org.ala.biocache.util.{JMX, OptionParser}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
 * A tool for processing records that are local to a node.
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
          skipDrs = v.trim.split(",").map {_.trim}
        }
      })
      opt("tl", "taxa-list", "file containing a list of taxa to reprocess", {
        v: String => taxaFile = v
      })
      intOpt("stk", "start-token-range-idx", "the idx of the token range to start at. Typically a value between 0 and 1024." +
        "This is useful when a long running process fails for some reason.", {
        v: Int => startTokenRangeIdx = v
      })
    }

    if(parser.parse(args)){
      if(taxaFile != ""){
        new ProcessLocalRecords().processTaxaOnly(threads, taxaFile, startTokenRangeIdx, checkpointFile)
      } else {
        new ProcessLocalRecords().processRecords(threads, drs, skipDrs, startTokenRangeIdx, checkpointFile)
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
      }

      true
    },
      threads,
      Array("rowkey", "taxonConceptID" + Config.persistenceManager.fieldDelimiter + "p")
    )

    //Move checkpoint file if complete
    new File(checkpointFile).renameTo(new File(checkpointFile + ".complete"))
    logger.info("Finished reprocessing. Total matched and reprocessed: " + count)
  }

  def processRecords(threads:Int, drs:Seq[String], skipDrs:Seq[String],
                     startTokenRangeIdx:Int, checkpointFile:String) : Unit = {

    val processor = new RecordProcessor
    val start = System.currentTimeMillis()
    var lastLog = System.currentTimeMillis()

    var updateCount = new AtomicLong(0)
    var updateFailCount = new AtomicLong(0)
    var readCount = new AtomicLong(0)

    setCheckpoints(startTokenRangeIdx, checkpointFile)

    val total = {
      logger.info("Using a full scan...")
      Config.occurrenceDAO.pageOverRawProcessedLocal(record => {
        if (!record.isEmpty) {
          val (raw, processed) = record.get
          val uuid = raw.rowKey
          readCount.incrementAndGet()

          if ((drs.isEmpty || drs.contains(raw.attribution.dataResourceUid)) &&
            !skipDrs.contains(raw.attribution.dataResourceUid)) {
            try {
              processor.processRecord(raw, processed, false, true)
              updateCount.incrementAndGet()
            } catch {
              case e:Exception => {
                logger.error("Problem processing record with UUID: "  + uuid, e)
                updateFailCount.incrementAndGet()
              }
            }
          }

          val lastReadCount = readCount.get()
          val lastUpdateCount = updateCount.get()
          val lastUpdateFailCount = updateFailCount.get()
          if (lastUpdateCount % 10000 == 0) {
            val end = System.currentTimeMillis()
            val timeInSecs = ((end - lastLog).toFloat / 1000f)
            val recordsPerSec = Math.round(10000f / timeInSecs)
            logger.info(s"Record/sec:$recordsPerSec,  updated:$lastUpdateCount, read:$lastReadCount, updateFail:$lastUpdateFailCount  Last rowkey: $uuid  Last 1000 in $timeInSecs")
            lastLog = end

            if(Config.jmxDebugEnabled){
              JMX.updateProcessingStats(
                recordsPerSec,
                timeInSecs,
                updateCount.intValue(),
                readCount.intValue()
              )

              val processorTimings = processor.getProcessTimings
              JMX.updateProcessingCacheStatistics(
                ClassificationDAO.getCacheSize,
                LocationDAO.getCacheSize,
                LocationDAO.getStoredPointCacheSize,
                AttributionDAO.getCacheSize,
                SpatialLayerDAO.getCacheSize,
                TaxonProfileDAO.getCacheSize,
                SensitivityDAO.getCacheSize,
                CommonNameDAO.getCacheSize,
                Config.persistenceManager.asInstanceOf[Cassandra3PersistenceManager].getCacheSize,
                processorTimings
              )
            }
          }
        }
        true
      }, null, threads)
    }

    //Move checkpoint file if complete
    new File(checkpointFile).renameTo(new File(checkpointFile + ".complete"))

    val end = System.currentTimeMillis()
    val timeInMinutes = ((end - start).toFloat / 100f / 60f / 60f)
    val timeInSecs = ((end - start).toFloat / 1000f)
    logger.info(s"Total records processed : $total in $timeInSecs seconds (or $timeInMinutes minutes) readCount=" + readCount.get() + " updateCount=" + updateCount.get() + " updateFailCount=" + updateFailCount.get())
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
