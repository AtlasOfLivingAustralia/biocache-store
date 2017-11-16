package au.org.ala.biocache.tool

import java.io.{FileWriter, File}
import java.util.concurrent.ConcurrentLinkedQueue

import au.org.ala.biocache.Config
import au.org.ala.biocache.caches.LocationDAO
import au.org.ala.biocache.index.BulkProcessor._
import au.org.ala.biocache.util.{Json, OptionParser}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

object SampleLocalRecords extends au.org.ala.biocache.cmd.Tool {

  def cmd = "sample-local-node"

  def desc = "Sample coordinates against geospatial layers for this node"

  protected val logger = LoggerFactory.getLogger("SampleLocalRecords")

  def main(args: Array[String]) {

    var locFilePath = ""
    var keepFiles = false
    var loadOccOnly = false
    var sampleOnly = false
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
      opt("load-occ-only", "Just run the step that loads occ with values from loc", {
        loadOccOnly = true
      })
      opt("sample-only", "Just run the step that samples and loads loc table", {
        sampleOnly = true
      })
    }

    if (parser.parse(args)) {
      new SampleLocalRecords().sample(workingDir, numThreads, keepFiles, loadOccOnly, sampleOnly)
    }
  }
}

class SampleLocalRecords {

  def sample(workingDir:String, threads:Int, keepFiles:Boolean, loadOccOnly:Boolean, sampleOnly:Boolean) : Unit = {

    val samplingFilePath = workingDir + "/sampling-local.txt"
    val locFilePath = workingDir + "/loc-local.txt"

    if(!loadOccOnly) {

      val fw = new FileWriter(locFilePath)
      val queue = new ConcurrentLinkedQueue[String]()

      //export from loc table to file
      Config.persistenceManager.pageOverSelect("loc", (key, map) => {
        val lat = map.getOrElse("lat", "")
        val lon = map.getOrElse("lon", "")
        if (lat != "" && lon != "") {
          queue.add(lon + "," + lat + "\n")
        }
        true
      },  1000, 1, "rowkey", "lat", "lon")

      val iter = queue.iterator()

      while (iter.hasNext) {
        fw.write(iter.next())
      }

      fw.flush
      fw.close

      //run sampling
      val sampling = new Sampling()
      //generate sampling
      sampling.sampling(locFilePath,
        samplingFilePath,
        singleLayerName = "",
        batchSize = 100000,
        concurrentLoading = true,
        keepFiles = true
      )
    }

    if(!sampleOnly) {
      //load sampling to occurrence records
      logger.info("Loading sampling into occ table")
      loadSamplingIntoOccurrences(threads)
      logger.info("Completed loading sampling into occ table")
    }

    //clean up the file
    if(!keepFiles && !loadOccOnly){
      logger.info(s"Removing temporary file: $samplingFilePath")
      (new File(samplingFilePath)).delete()
      if (new File(locFilePath).exists()) (new File(locFilePath)).delete()
    }
  }

  def loadSamplingIntoOccurrences(threads:Int) : Unit = {
    logger.info(s"Starting loading sampling for local records")
    Config.persistenceManager.pageOverLocal("occ", (guid, map, tokenRangeIdx) => {
      val lat = map.getOrElse("decimallatitude" + Config.persistenceManager.fieldDelimiter + "p", "")
      val lon = map.getOrElse("decimallongitude" + Config.persistenceManager.fieldDelimiter + "p", "")
      if (lat != "" && lon != "") {
        val point = LocationDAO.getSamplesForLatLon(lat, lon)
        if (!point.isEmpty) {
          val (location, environmentalLayers, contextualLayers) = point.get
          Config.persistenceManager.put(guid, "occ", Map(
            "el" + Config.persistenceManager.fieldDelimiter + "p" -> Json.toJSON(environmentalLayers),
            "cl" + Config.persistenceManager.fieldDelimiter + "p" -> Json.toJSON(contextualLayers)),
            false,
            false
          )
        } else {
          logger.info(s"[Loading sampling] Missing sampled values for $guid, with $lat, $lon")
        }
        counter += 1
        if (counter % 1000 == 0) {
          logger.info(s"[Loading sampling] Import of sample data $counter Last key $guid")
        }
      }
      true
    }, threads,
      Array(
        "rowkey",
        "decimalLatitude" + Config.persistenceManager.fieldDelimiter + "p",
        "decimalLongitude" + Config.persistenceManager.fieldDelimiter + "p"
      )
    )
  }
}
