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
    val parser = new OptionParser(help) {
      intOpt("t", "thread", "The number of threads to use", {v:Int => threads = v } )
    }
    if(parser.parse(args)){
      new ProcessLocalRecords().processRecords(threads)
    }
  }
}

/**
  * Created by mar759 on 29/07/2016.
  */
class ProcessLocalRecords {

  val logger = LoggerFactory.getLogger("ProcessLocalRecords")

  def processRecords(threads:Int): Unit = {

    val processor = new RecordProcessor
    val start = System.currentTimeMillis()

    val total = Config.occurrenceDAO.pageOverRawProcessedLocal(record => {
      if(!record.isEmpty){
        val raw = record.get._1
        val (processed, assertions) = processor.processRecord(raw)
        Config.occurrenceDAO.updateOccurrence(raw.getRowKey, processed, Versions.PROCESSED)
      }
      true
    }, threads)

    val end = System.currentTimeMillis()
    logger.info("Total records processed : " + total + " in " + ((end-start).toFloat / 60f /60f) + " minutes")
  }
}
