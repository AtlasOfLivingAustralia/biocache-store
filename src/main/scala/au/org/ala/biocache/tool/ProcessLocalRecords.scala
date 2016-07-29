package au.org.ala.biocache.tool

import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.model.Versions
import au.org.ala.biocache.processor.RecordProcessor
import au.org.ala.biocache.util.OptionParser

object ProcessLocalRecords extends Tool {

  def cmd = "process-local-node"
  def desc = "Process all records on a local node"

  def main(args:Array[String]){

    var threads:Int = 1
    var address:String = "127.0.0.1"
    val parser = new OptionParser(help) {
      intOpt("t", "thread", "The number of threads to use", {v:Int => threads = v } )
      opt("ip", "local-ip-node", "The address", {v:String => address = v } )
    }
    if(parser.parse(args)){
      new ProcessLocalRecords().processRecords(threads, address)
    }
  }
}

/**
  * Created by mar759 on 29/07/2016.
  */
class ProcessLocalRecords {

  def processRecords(threads:Int, address:String): Unit = {
    val processor = new RecordProcessor
    var counter = 0
    Config.occurrenceDAO.pageOverRawProcessedLocal(record => {
      if(!record.isEmpty){
        val raw = record.get._1
        val (processed, assertions) = processor.processRecord(raw)
        Config.occurrenceDAO.updateOccurrence(raw.getRowKey, processed, Versions.PROCESSED)
      }
      true
    }, threads, address)
  }
}
