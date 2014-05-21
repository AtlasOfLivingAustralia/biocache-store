package au.org.ala.biocache.load

import au.org.ala.biocache.util.OptionParser
import au.org.ala.biocache.tool.{ProcessWithActors, Sampling}
import au.org.ala.biocache.index.IndexRecords
import au.org.ala.biocache.cmd.Tool
import org.slf4j.LoggerFactory

object IngestTool extends Tool {

  val logger = LoggerFactory.getLogger("IngestTool")
  def cmd = "ingest"
  def desc = "Load, sample, process and index a dataset"

  def main(args:Array[String]){

    var resources = Array[String]()
    var ingestAll = false

    val parser = new OptionParser(help) {
      opt("r", "dr", "comma separated list of resources (uids) to load, sample, process and index. e.g. dr321,dr123", {
        v: String => resources = v.split(",").map(x => x.trim)
      })
      booleanOpt("all", "all-resources", "flag to indicate all resources should be loaded", {
        v: Boolean => ingestAll = v
      })
    }
    if(parser.parse(args)){
      val l = new Loader

      if(!resources.isEmpty) {
        resources.foreach(uid => {
          logger.info(s"Ingesting resource uid: $uid")
          if (uid != "") {
            ingestResource(uid)
          }
        })
      } else if(ingestAll){
        (new Loader()).resourceList.foreach(resource => {
          val uid = resource.getOrElse("uid", "")
          val name = resource.getOrElse("name", "")
          logger.info(s"Ingesting resource $name, uid: $uid")
          if(uid != ""){
            ingestResource(uid)
          }
        })
      } else {
        parser.showUsage
      }
    }
  }

  def ingestResource(uid: String) {
    logger.info("Loading: " + uid)
    (new Loader()).load(uid)
    logger.info("Sampling: " + uid)
    Sampling.main(Array("-dr", uid))
    logger.info("Processing: " + uid)
    ProcessWithActors.processRecords(4, None, Some(uid))
    logger.info("Indexing: " + uid)
    IndexRecords.index(None, None, Some(uid), false, false)
    logger.info("Finished ingest for: " + uid)
  }
}