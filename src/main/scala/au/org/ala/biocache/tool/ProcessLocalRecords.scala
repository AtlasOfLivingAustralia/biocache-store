package au.org.ala.biocache.tool

import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.model.Versions
import au.org.ala.biocache.processor.RecordProcessor
import au.org.ala.biocache.util.OptionParser
import org.slf4j.LoggerFactory

object ProcessLocalRecords extends Tool {

  def cmd = "process-local-node"
  def desc = "Process all records on a local database node"

  def main(args:Array[String]){

    var threads:Int = 1
    var drs:Seq[String] = List()
    var skipDrs:Seq[String] = List()
    var useFullScan = false
    var startTokenRangeIdx = 0

    val parser = new OptionParser(help) {
      intOpt("t", "thread", "The number of threads to use", {v:Int => threads = v } )
      opt("dr", "data-resource-list", "comma separated list of drs to process", {
        v: String => drs = v.trim.split(",").map {_.trim}
      })
      opt("edr", "skip-data-resource-list", "comma separated list of drs to NOT process", {
        v: String => {
          skipDrs = v.trim.split(",").map {_.trim}
          useFullScan = true
        }
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

    if(skipDrs.isEmpty && drs.size == 1){
      useFullScan = false
    }

    if(drs.isEmpty || !skipDrs.isEmpty){
      useFullScan = true
    }

    if(parser.parse(args)){
      new ProcessLocalRecords().processRecords(threads, drs, skipDrs, useFullScan, startTokenRangeIdx)
    }
  }
}

/**
  * A class for processing local records for this node.
  */
class ProcessLocalRecords {

  val logger = LoggerFactory.getLogger("ProcessLocalRecords")

  def processRecords(threads:Int, drs:Seq[String], skipDrs:Seq[String], useFullScan:Boolean, startTokenRangeIdx:Int) : Unit = {

    val processor = new RecordProcessor
    val start = System.currentTimeMillis()
    var lastLog = System.currentTimeMillis()
    val processTime = org.apache.commons.lang.time.DateFormatUtils.format(new java.util.Date, "yyyy-MM-dd'T'HH:mm:ss'Z'")

    //note this update count isn't thread safe, so its inaccurate
    //its been left in to give a general idea of performance
    var updateCount = 0
    var readCount = 0

    System.setProperty("startAtTokenRange", startTokenRangeIdx.toString)

    val total = if(useFullScan) {
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
            processed.lastModifiedTime = org.apache.commons.lang.time.DateFormatUtils.format(new java.util.Date, "yyyy-MM-dd'T'HH:mm:ss'Z'")
            Config.occurrenceDAO.updateOccurrence(rowKey, processed, Some(assertions), Versions.PROCESSED)
            updateCount += 1
          }
          if (updateCount % 1000 == 0) {
            val end = System.currentTimeMillis()
            val timeInSecs = ((end - lastLog).toFloat / 1000f)
            val recordsPerSec = Math.round(1000f / timeInSecs)
            logger.info(s"Total processed : $updateCount, total read: $readCount Last rowkey: $rowKey  Last 1000 in $timeInSecs seconds ($recordsPerSec records a second)")
            lastLog = end
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

    val end = System.currentTimeMillis()
    val timeInMinutes = ((end-start).toFloat / 100f / 60f / 60f)
    val timeInSecs = ((end-start).toFloat / 1000f  )
    logger.info(s"Total records processed : $total in $timeInSecs seconds (or $timeInMinutes minutes)")
  }
}
