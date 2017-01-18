package au.org.ala.biocache.tool

import java.io.{FileWriter, File}

import au.org.ala.biocache.Config
import au.org.ala.biocache.caches.LocationDAO
import au.org.ala.biocache.index.BulkProcessor._
import au.org.ala.biocache.util.{Json, OptionParser}
import org.slf4j.LoggerFactory

object SampleLocalRecords extends au.org.ala.biocache.cmd.Tool {

  def cmd = "sample-local-node"

  def desc = "Sample coordinates against geospatial layers for this node"

  protected val logger = LoggerFactory.getLogger("SampleLocalRecords")

  def main(args: Array[String]) {

    var locFilePath = ""
    var keepFiles = false
    var workingDir = Config.tmpWorkDir
    var batchSize = 100000
    var numThreads = 1

    val parser = new OptionParser(help) {
      opt("cf", "coordinates-file", "the file containing coordinates", {
        v: String => locFilePath = v
      })
      opt("keep", "Keep the files produced from the sampling", {
        keepFiles = true
      })
      opt("wd", "working-dir", "the directory to write temporary files too. Defaults to " + Config.tmpWorkDir, {
        v: String => workingDir = v
      })
      intOpt("bs", "batch-size", "Batch size when processing points. Defaults to " + batchSize, {
        v: Int => batchSize = v
      })
      intOpt("t", "threads", "The number of threads for the unique coordinate extract. The default is " + numThreads, {
        v: Int => numThreads = v
      })
    }

    if (parser.parse(args)) {
      new SampleLocalRecords().sample(workingDir, numThreads, keepFiles)
    }
  }
}

class SampleLocalRecords {


  def sample(workingDir:String, threads:Int, keepFiles:Boolean) : Unit = {

    val locFilePath = workingDir + "/loc-local.txt"
    val fw = new FileWriter(locFilePath)

    //export from loc table to file
    Config.persistenceManager.pageOverLocal("loc", (key, map) => {
      val lat = map.getOrElse("lat", "")
      val lon = map.getOrElse("lon", "")
      if(lat != "" && lon != ""){
        fw.write(lon)
        fw.write(",")
        fw.write(lat)
        fw.write("\n")
        fw.flush
      }
      true
    }, threads, Array("rowkey", "lat", "lon"))

    fw.close

    //run sampling
    val sampling = new Sampling()

    val samplingFilePath = workingDir + "/sampling-local.txt"

    //generate sampling
    sampling.sampling(locFilePath,
      samplingFilePath,
      singleLayerName="",
      batchSize=100000,
      concurrentLoading=true,
      keepFiles=true
    )

    //load sampling to occurrence records
    logger.info("Loading sampling into occ table")
    loadSamplingIntoOccurrences(threads)
    logger.info("Completed loading sampling into occ table")

    //clean up the file
    if(!keepFiles){
      logger.info(s"Removing temporary file: $samplingFilePath")
      (new File(samplingFilePath)).delete()
      if (new File(locFilePath).exists()) (new File(locFilePath)).delete()
    }
  }

  def loadSamplingIntoOccurrences(threads:Int) : Unit = {
    logger.info(s"Starting loading sampling for local records")
    Config.persistenceManager.pageOverLocal("occ", (guid, map) => {
      val lat = map.getOrElse("decimalLatitude" + Config.persistenceManager.fieldDelimiter + "p", "")
      val lon = map.getOrElse("decimalLongitude" + Config.persistenceManager.fieldDelimiter + "p", "")
      if (lat != null && lon != null) {
        val point = LocationDAO.getSamplesForLatLon(lat, lon)
        if (!point.isEmpty) {
          val (location, environmentalLayers, contextualLayers) = point.get
          Config.persistenceManager.put(guid, "occ", Map(
            "el" + Config.persistenceManager.fieldDelimiter + "p" -> Json.toJSON(environmentalLayers),
            "cl" + Config.persistenceManager.fieldDelimiter + "p" -> Json.toJSON(contextualLayers)),
            false,
            false
          )
        }
        counter += 1
        if (counter % 1000 == 0) {
          logger.info(s"[Loading sampling] Import of sample data $counter Last key $guid")
        }
      }
      true
    }, threads, Array("rowkey", "decimalLatitude" + Config.persistenceManager.fieldDelimiter + "p", "decimalLongitude" + Config.persistenceManager.fieldDelimiter + "p"))
  }
}
