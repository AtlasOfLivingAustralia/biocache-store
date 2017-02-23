package au.org.ala.biocache.tool

import java.io.FileWriter

import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.model.{Versions, FullRecord}
import au.org.ala.biocache.processor.AttributionProcessor
import au.org.ala.biocache.util.OptionParser
import org.slf4j.LoggerFactory

/**
  * A utility to refresh attribution information without running a full reprocess.
  */
object UpdateAttribution extends Tool {

  def cmd = "update-attribution"
  def desc = "Update the attribution for the records on a node. This is faster than full reprocess."

  def main(args:Array[String]){

    var threads:Int = 1
    var uuid:String = null
    var updateLog:FileWriter = null
    var skippedLog:FileWriter = null
    val parser = new OptionParser(help) {
      intOpt("t", "thread", "The number of threads to use", {v:Int => threads = v } )
      opt("uuid", "uuid", "Just update a single record", {
        v: String => uuid = v
      })
      opt("ul", "update-log-file", "Update log file", {
        v: String => updateLog = new FileWriter(v)
      })
      opt("sl", "skipped-log-file", "Skipped log file", {
        v: String => skippedLog = new FileWriter(v)
      })
    }
    if(parser.parse(args)){
      if(uuid != null){
        new UpdateAttribution().updateSingleRecord(uuid)
      } else {
        new UpdateAttribution().updateRecords(threads, updateLog, skippedLog)
      }
    }
  }
}

/**
  * A utility to refresh attribution information without running a full reprocess.
  */
class UpdateAttribution {

  val logger = LoggerFactory.getLogger("UpdateAttribution")

  val ap = new AttributionProcessor

  def updateRecords(threads:Int, updateLog:FileWriter, skippedLog:FileWriter) : Unit = {
    val curried = updateAttribution(updateLog, skippedLog, debug=false) _
    Config.occurrenceDAO.pageOverRawProcessedLocal(curried, null, threads)
    if(updateLog != null) {
      updateLog.flush()
      updateLog.close()
    }
    if(skippedLog != null) {
      skippedLog.flush()
      skippedLog.close()
    }
    logger.info(s"Update complete")
  }

  def updateSingleRecord(uuid:String): Unit = {
    val rawProcessed = Config.occurrenceDAO.getRawProcessedByRowKey(uuid)
    if(!rawProcessed.isEmpty){
      val raw = rawProcessed.get(0)
      val processed = rawProcessed.get(1)
      updateAttribution(null, null, debug=true)(Some((raw, processed)))
    } else {
      logger.warn(s"$uuid not recognised.")
    }
  }

  def updateAttribution(updateLog:FileWriter, skippedLog:FileWriter, debug:Boolean = false)(rawProcess:Option[(FullRecord, FullRecord)]) : Boolean = {
    if(!rawProcess.isEmpty){

      val raw = rawProcess.get._1
      val lastProcessed = rawProcess.get._2
      val oldAttribution = lastProcessed.attribution.toMap

      ap.process(raw.rowKey, raw, lastProcessed, Some(lastProcessed))

      val newAttribution = lastProcessed.attribution.toMap

      //compare the maps
      var updateRequired = false
      newAttribution.keySet.foreach { key =>
        val oldValue = oldAttribution.getOrElse(key, "")
        val newValue = newAttribution.getOrElse(key, "")
        val diff = oldValue != newValue
        if(debug){
          println(s"$key old: $oldValue new: $newValue diff: $diff")
        }
        if(diff){
          updateRequired = true
        }
      }
      if(debug) {
        println(s"Update required: $updateRequired")
      }

      if(updateRequired){
        val mapToPersist = lastProcessed.attribution.toMap.map { case (k, v) =>
          (k + Config.persistenceManager.fieldDelimiter + "p" -> v)
        }
        //look for changes
        Config.persistenceManager.put(raw.rowKey, "occ", mapToPersist, true, true)
        if(updateLog != null){
          synchronized { updateLog.write(raw.rowKey + "\n") }
        }
      } else if(skippedLog != null){
        synchronized { skippedLog.write(raw.rowKey + "\n") }
      }
    }
    true
  }

}
