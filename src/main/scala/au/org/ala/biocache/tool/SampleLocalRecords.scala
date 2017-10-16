package au.org.ala.biocache.tool

import java.io.{File, FileWriter}
import java.util

import au.org.ala.biocache.Config
import au.org.ala.biocache.caches.LocationDAO
import au.org.ala.biocache.index.BulkProcessor._
import au.org.ala.biocache.util.{Json, OptionParser, ZookeeperUtil}
import org.slf4j.LoggerFactory

import scala.io.Source

object SampleLocalRecords extends au.org.ala.biocache.cmd.Tool {

  def cmd = "sample-local-node"

  def desc = "Sample coordinates against geospatial layers for this node"

  protected val logger = LoggerFactory.getLogger("SampleLocalRecords")

  def main(args: Array[String]) {

    var locFilePath = ""
    var keepFiles = false
    var loadOccOnly = false
    var sampleOnly = false
    var rebuildLoc = false
    var workingDir = Config.tmpWorkDir
    var batchSize = 100000
    var numThreads = 1
    var layers: Seq[String] = List()

    var drs: Seq[String] = List()
    var skipDrs: Seq[String] = List()
    var useFullScan = false
    var startTokenRangeIdx = 0
    var taxaFile = ""

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
      opt("rebuild-loc", "Fix loc table when it is missing entries", {
        rebuildLoc = true
      })
      opt("load-occ-only", "Just run the step that loads occ with values from loc", {
        loadOccOnly = true
      })
      opt("sample-only", "Just run the step that samples and loads loc table", {
        sampleOnly = true
      })
      opt("l", "layers", "Comma separated list of el and cl layer names to sample", {
        v: String =>
          layers = v.trim.split(",").map {
            _.trim
          }
      })

      //replicate process-local-node filtering parameters
      opt("dr", "data-resource-list", "comma separated list of drs to process", {
        v: String =>
          drs = v.trim.split(",").map {
            _.trim
          }
      })
      opt("edr", "skip-data-resource-list", "comma separated list of drs to NOT process", {
        v: String => {
          skipDrs = v.trim.split(",").map {
            _.trim
          }
          useFullScan = true
        }
      })
      opt("tl", "taxa-list", "file containing a list of taxa to reprocess", {
        v: String => taxaFile = v
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

    if (parser.parse(args)) {
      if (taxaFile != "") {
        new SampleLocalRecords().sampleTaxaOnly(workingDir, numThreads, keepFiles, loadOccOnly, sampleOnly, rebuildLoc, taxaFile, startTokenRangeIdx, layers)
      } else {
        new SampleLocalRecords().sampleRecords(workingDir, numThreads, keepFiles, loadOccOnly, sampleOnly, rebuildLoc, drs, skipDrs, useFullScan, startTokenRangeIdx, layers)
      }
    }
  }
}

class SampleLocalRecords {

  def sample(workingDir: String, threads: Int, keepFiles: Boolean, loadOccOnly: Boolean, sampleOnly: Boolean,
             rebuildLoc: Boolean, queue: util.HashSet[String], layers: Seq[String]): Unit = {

    val samplingFilePath = workingDir + "/sampling-local.txt"
    val locFilePath = workingDir + "/loc-local.txt"

    if (rebuildLoc) {
      fixLocTable(threads)
    }

    if (!loadOccOnly) {

      val fw = new FileWriter(locFilePath)
      val queue = new util.HashSet[String]()

      //export from loc table to file
      Config.persistenceManager.pageOverSelect("loc", (key, map) => {
        val lat = map.getOrElse("lat", "")
        val lon = map.getOrElse("lon", "")
        if (lat != "" && lon != "") {
          queue.add(lon + "," + lat + "\n")
        }
        true
      }, "", "", 1000, "rowkey", "lat", "lon")

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
        batchSize = 100000,
        concurrentLoading = true,
        keepFiles = true,
        layers = layers
      )
    }

    if (!sampleOnly) {
      //load sampling to occurrence records
      logger.info("Loading sampling into occ table")
      loadSamplingIntoOccurrences(threads)
      logger.info("Completed loading sampling into occ table")
    }

    //clean up the file
    if (!keepFiles && !loadOccOnly) {
      logger.info(s"Removing temporary file: $samplingFilePath")
      (new File(samplingFilePath)).delete()
      if (new File(locFilePath).exists()) (new File(locFilePath)).delete()
    }

    ZookeeperUtil.setStatus("PROCESSING", "COMPLETED", queue.size())
  }

  def fixLocTable(threads: Int): Unit = {
    val queue = new util.HashSet[String]()

    logger.info("Fixing loc table...")
    Config.occurrenceDAO.pageOverRawProcessedLocal(record => {
      if (!record.isEmpty) {
        val lat: String = record.getOrElse("decimalLatitude" + Config.persistenceManager.fieldDelimiter + "p", "").toString()
        val lon: String = record.getOrElse("decimalLongitude" + Config.persistenceManager.fieldDelimiter + "p", "").toString()
        if (!lat.isEmpty && !lon.isEmpty) {
          queue.add(lon + "," + lat)
        }
      }
      true
    }, null, threads)

    val it = queue.iterator()
    while (it.hasNext) {
      val coords = it.next().split(",")
      LocationDAO.storePointForSampling(coords(1), coords(0))
    }
  }

  /**
    * Reads the taxonIDs supplied in the file and only sample records that match this list.
    *
    * @param threads
    * @param taxaFilePath
    * @param startTokenRangeIdx
    */
  def sampleTaxaOnly(workingDir: String, threads: Int, keepFiles: Boolean, loadOccOnly: Boolean, sampleOnly: Boolean,
                     rebuildLoc: Boolean, taxaFilePath: String, startTokenRangeIdx: Int, layers: Seq[String]): Unit = {

    ZookeeperUtil.setStatus("SAMPLING", "STARTING", 0)
    //read the taxa file
    val taxaIDList = Source.fromFile(new File(taxaFilePath)).getLines().toSet[String]
    logger.info("Number of taxa to process " + taxaIDList.size)

    var count = 0

    val queue = new util.HashSet[String]()

    Config.persistenceManager.pageOverLocal("occ", (rowkey, map, _) => {
      val lat = map.getOrElse("lat", "")
      val lon = map.getOrElse("lon", "")
      if (lat != "" && lon != "") {
        queue.add(lon + "," + lat + "\n")
      }
      count += 1
      if (count % 100000 == 0) {
        logger.info(s"Total read : $count")
        ZookeeperUtil.setStatus("SAMPLING", "RUNNING", count)
      }

      true
    },
      threads,
      Array("rowkey", "lat", "lon")
    )

    sample(workingDir, threads, keepFiles, loadOccOnly, sampleOnly, rebuildLoc, queue, layers)
  }

  def sampleRecords(workingDir: String, threads: Int, keepFiles: Boolean, loadOccOnly: Boolean, sampleOnly: Boolean,
                    rebuildLoc: Boolean, drs: Seq[String], skipDrs: Seq[String], useFullScan: Boolean, startTokenRangeIdx: Int,
                    layers: Seq[String]): Unit = {

    val start = System.currentTimeMillis()
    var lastLog = System.currentTimeMillis()

    //note this update count isn't thread safe, so its inaccurate
    //its been left in to give a general idea of performance
    var updateCount = 0
    var readCount = 0

    val queue = new util.HashSet[String]()

    val total = if (useFullScan) {
      logger.info("Using a full scan...")
      Config.occurrenceDAO.pageOverRawProcessedLocal(record => {
        if (!record.isEmpty) {
          val raw = record.get._1
          val rowKey = raw.rowKey
          readCount += 1
          if ((drs.isEmpty || drs.contains(raw.attribution.dataResourceUid)) &&
            !skipDrs.contains(raw.attribution.dataResourceUid)) {

            val lat = record.getOrElse("decimalLatitude" + Config.persistenceManager.fieldDelimiter + "p", "")
            val lon = record.getOrElse("decimalLongitude" + Config.persistenceManager.fieldDelimiter + "p", "")
            if (lat != "" && lon != "") {
              queue.add(lon + "," + lat + "\n")
            }

            updateCount += 1
          }
          if (updateCount % 10000 == 0) {

            val end = System.currentTimeMillis()
            val timeInSecs = ((end - lastLog).toFloat / 10000f)
            val recordsPerSec = Math.round(10000f / timeInSecs)
            logger.info(s"Total processed : $updateCount, total read: $readCount Last rowkey: $rowKey  Last 1000 in $timeInSecs seconds ($recordsPerSec records a second)")
            lastLog = end
            ZookeeperUtil.setStatus("SAMPING", "RUNNING", updateCount)
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

            val lat = record.getOrElse("decimalLatitude" + Config.persistenceManager.fieldDelimiter + "p", "")
            val lon = record.getOrElse("decimalLongitude" + Config.persistenceManager.fieldDelimiter + "p", "")
            if (lat != "" && lon != "") {
              queue.add(lon + "," + lat + "\n")
            }

            updateCount += 1

            if (updateCount % 100 == 0 && updateCount > 0) {
              val raw = record.get._1
              val rowKey = raw.rowKey
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

    sample(workingDir, threads, keepFiles, loadOccOnly, sampleOnly, rebuildLoc, queue, layers)
  }

  def loadSamplingIntoOccurrences(threads: Int): Unit = {
    logger.info(s"Starting loading sampling for local records")
    Config.persistenceManager.pageOverLocal("occ", (guid, map, _) => {
      val lat = map.getOrElse("decimalLatitude" + Config.persistenceManager.fieldDelimiter + "p", "")
      val lon = map.getOrElse("decimalLongitude" + Config.persistenceManager.fieldDelimiter + "p", "")
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
