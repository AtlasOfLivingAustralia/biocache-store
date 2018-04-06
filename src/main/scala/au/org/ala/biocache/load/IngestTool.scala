package au.org.ala.biocache.load

import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.index.IndexRecords
import au.org.ala.biocache.tool.{ProcessRecords, Sampling}
import au.org.ala.biocache.util.OptionParser
import org.slf4j.LoggerFactory

/**
  * Command line tool for ingesting data resource with options to skip
  * sections of the loading process. This can be used to load single resources, but isnt recommended for ingesting
  * millions of records.
  */
object IngestTool extends Tool {

  val logger = LoggerFactory.getLogger("IngestTool")

  def cmd = "ingest"

  def desc = "Load, sample, process and index a dataset"

  def main(args: Array[String]) {

    var resources = Array[String]()
    var ingestAll = false
    var skipLoading = false
    var skipSampling = false
    var skipProcessing = false
    var skipIndexing = false
    var threads: Int = 4

    val parser = new OptionParser(help) {
      opt("dr", "dataResourceUid", "comma separated list of resources (uids) to load, sample, process and index. e.g. dr321,dr123", {
        v: String => resources = v.split(",").map(x => x.trim)
      })
      opt("all", "all-resources", "flag to indicate all resources should be loaded", {
        ingestAll = true
      })
      opt("skip-loading", "Ingest but don't load.", {
        skipLoading = true
      })
      opt("skip-sampling", "Ingest but don't sample.", {
        skipSampling = true
      })
      opt("skip-processing", "Ingest but don't process.", {
        skipProcessing = true
      })
      opt("skip-indexing", "Ingest but don't indexing.", {
        skipIndexing = true
      })
      intOpt("t", "threads", "Number of threads to index from", { v: Int => threads = v })
    }

    if (parser.parse(args)) {
      val l = new Loader

      if (!resources.isEmpty) {
        resources.foreach(uid => {
          logger.info(s"Ingesting resource uid: $uid")
          if (uid != "") {
            IngestTool.ingestResource(
              uid,
              threads,
              skipLoading = skipLoading,
              skipProcessing = skipProcessing,
              skipSampling = skipSampling,
              skipIndexing = skipIndexing
            )
          }
        })
      } else if (ingestAll) {
        (new Loader()).resourceList.foreach(resource => {
          val uid = resource.getOrElse("uid", "")
          val name = resource.getOrElse("name", "")
          logger.info(s"Ingesting resource $name, uid: $uid")
          if (uid != "") {
            IngestTool.ingestResource(
              uid,
              threads,
              skipLoading = skipLoading,
              skipProcessing = skipProcessing,
              skipSampling = skipSampling,
              skipIndexing = skipIndexing
            )
          }
        })
      } else {
        parser.showUsage
      }
    }
  }

  /**
    * Ingest a resource.
    *
    * @param uid
    * @param skipIndexing
    * @param skipLoading
    * @param skipProcessing
    * @param skipSampling
    */
  def ingestResource(uid: String,
                     threads: Int,
                     skipIndexing: Boolean = false,
                     skipLoading: Boolean = false,
                     skipProcessing: Boolean = false,
                     skipSampling: Boolean = false
                    ) {
    if (!skipLoading) {
      logger.info("Loading: " + uid)
      (new Loader()).load(uid)
    } else {
      logger.info("Skipping loading: " + uid)
    }
    if (!skipProcessing) {
      logger.info("Processing: " + uid)
      ProcessRecords.processRecords(uid, threads)
    } else {
      logger.info("Skipping processing: " + uid)
    }
    if (!skipSampling) {
      logger.info("Sampling: " + uid)
      Sampling.main(Array("-dr", uid, "--crk"))
    } else {
      logger.info("Skipping sampling: " + uid)
    }
    if (!skipIndexing) {
      logger.info("Indexing: " + uid)
      IndexRecords.main(Array("-dr", uid, "--acrk"))
    } else {
      logger.info("Skipping indexing: " + uid)
    }
    logger.info("Finished ingest for: " + uid)
  }
}