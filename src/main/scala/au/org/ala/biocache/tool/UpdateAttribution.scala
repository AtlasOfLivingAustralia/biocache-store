package au.org.ala.biocache.tool

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
  def desc = "Update the attribution for the records on a node. This is fastest than full reprocess."

  def main(args:Array[String]){

    var threads:Int = 1
    val parser = new OptionParser(help) {
      intOpt("t", "thread", "The number of threads to use", {v:Int => threads = v } )
    }
    if(parser.parse(args)){
      new UpdateAttribution().updateRecords(threads)
    }
  }
}

/**
  * A utility to refresh attribution information without running a full reprocess.
  */
class UpdateAttribution {

  val logger = LoggerFactory.getLogger("UpdateAttribution")

  def updateRecords(threads:Int) : Unit = {

    val start = System.currentTimeMillis()
    val ap = new AttributionProcessor

    Config.occurrenceDAO.pageOverRawProcessedLocal({ rawProcess =>

      if(!rawProcess.isEmpty){

        val raw = rawProcess.get._1
        val lastProcessed = rawProcess.get._2
        val oldAttribution = lastProcessed.attribution.toMap

        ap.process(raw.rowKey, raw, lastProcessed, Some(lastProcessed))

        val newAttribution = lastProcessed.attribution.toMap

        //compare the maps
        var updateRequired = false
        oldAttribution.keySet.foreach { key =>
          if(oldAttribution.getOrElse(key, "") != newAttribution.getOrElse(key, "")){
            updateRequired = true
          }
        }

        if(updateRequired){
          val mapToPersist = lastProcessed.attribution.toMap.map { case (k, v) => (k + Config.persistenceManager.fieldDelimiter + "p" -> v) }
          //look for changes
          Config.persistenceManager.put(raw.rowKey, "occ", mapToPersist, true, true)
        }
      }

      true
    }, threads)

    val end = System.currentTimeMillis()
    logger.info(s"Update complete")
  }
}
