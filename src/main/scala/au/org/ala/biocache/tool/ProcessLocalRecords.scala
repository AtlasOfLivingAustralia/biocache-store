package au.org.ala.biocache.tool

import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.model.Versions
import au.org.ala.biocache.processor.RecordProcessor
import au.org.ala.biocache.util.OptionParser
import org.slf4j.LoggerFactory

object ProcessLocalRecords extends Tool {

  def cmd = "process-local-node"
  def desc = "Process all records on a local node"

  def main(args:Array[String]){

    var threads:Int = 1
    var drs:Seq[String] = List()
    var skipDrs:Seq[String] = List()

    val parser = new OptionParser(help) {
      intOpt("t", "thread", "The number of threads to use", {v:Int => threads = v } )
      opt("dr", "data-resource-list", "comma separated list of drs to process", {
        v: String => drs = v.trim.split(",").map {_.trim}
      })
      opt("edr", "skip-data-resource-list", "comma separated list of drs to NOT process", {
        v: String => skipDrs = v.trim.split(",").map {_.trim}
      })
    }
    if(parser.parse(args)){
      new ProcessLocalRecords().processRecords(threads, drs, skipDrs)
    }
  }
}

/**
  * A class for processing local records for this node.
  */
class ProcessLocalRecords {

  val logger = LoggerFactory.getLogger("ProcessLocalRecords")

  def processRecords(threads:Int, drs:Seq[String], skipDrs:Seq[String]) : Unit = {

    val processor = new RecordProcessor
    val start = System.currentTimeMillis()
    var lastLog = System.currentTimeMillis()

    //note this update count isnt threadsafe, so its inaccurate
    //its been left in to give a general idea of performance
    var updateCount = 0
    var readCount = 0

    val total = Config.occurrenceDAO.pageOverRawProcessedLocal(record => {
      if(!record.isEmpty){
        val raw = record.get._1
        val rowkey = raw.rowKey
        readCount += 1
        if((drs.isEmpty || drs.contains(raw.attribution.dataResourceUid)) &&
          !skipDrs.contains(raw.attribution.dataResourceUid)){
          val (processed, assertions) = processor.processRecord(raw)
          Config.occurrenceDAO.updateOccurrence(raw.rowKey, processed, Versions.PROCESSED)
          updateCount += 1
        }
        if(updateCount % 1000 == 0){
          val end = System.currentTimeMillis()
          val timeInSecs = ((end-lastLog).toFloat / 1000f  )
          val recordsPerSec = Math.round(1000f/timeInSecs)
          logger.info(s"Total processed : $updateCount, total read: $readCount Last rowkey: $rowkey  Last 1000 in $timeInSecs seconds ($recordsPerSec records a second)")
          lastLog = end
        }
      }
      true
    }, threads)

    val end = System.currentTimeMillis()
    val timeInMinutes = ((end-start).toFloat / 100f / 60f / 60f)
    val timeInSecs = ((end-start).toFloat / 1000f  )
    logger.info(s"Total records processed : $total in $timeInSecs seconds (or $timeInMinutes minutes)")
  }
}
