package au.org.ala.biocache.tool

import java.io.{File, FileWriter}
import java.util

import au.org.ala.biocache.{Config, Store}
import au.org.ala.biocache.caches.LocationDAO
import au.org.ala.biocache.index.Counter
import au.org.ala.biocache.persistence.Cassandra3PersistenceManager
import au.org.ala.biocache.util.{Json, OptionParser, ZookeeperUtil}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters

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
    var layers: Seq[String] = List()

    var drs: Seq[String] = List()
    var skipDrs: Seq[String] = List()
    var useFullScan = false
    var startTokenRangeIdx = 0
    var taxaFile = ""

    var allNodes = false

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
      opt("use-full-scan", "Use a full table scan. This is faster if most of the data needs to be processed. When not used the default is to scan by data resource which is faster for smaller datasets (< 5 million records). ", {
        useFullScan = true
      })
      intOpt("stk", "start-token-range-idx", "the idx of the token range to start at. Typically a value between 0 and 1024." +
        "This is useful when a long running process fails for some reason.", {
        v: Int => startTokenRangeIdx = v
      })

      opt("all-nodes", "Run on all nodes, not just the local node. For use with --sampling-only to reduce load on the sampling service when many records (> 5 million records).", {
        allNodes = true
      })
    }

    if (parser.parse(args)) {
      new SampleLocalRecords().sampleRecords(workingDir, numThreads, keepFiles, loadOccOnly, sampleOnly, drs, skipDrs, useFullScan, startTokenRangeIdx, layers, allNodes)
    }
  }
}

class SampleLocalRecords extends Counter {

  def sample(workingDir: String, threads: Int, keepFiles: Boolean, loadOccOnly: Boolean, sampleOnly: Boolean,
             queue: util.HashSet[String], rowkeys: Seq[String], layers: Seq[String], allNodes: Boolean): Unit = {

    val samplingFilePath = workingDir + "/sampling-local.txt"
    val locFilePath = workingDir + "/loc-local.txt"

    if (!loadOccOnly) {
      val fw = new FileWriter(locFilePath)
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
      loadSamplingIntoOccurrences(threads, rowkeys, allNodes)
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


  /**
    * sampling and loading
    *
    * Sampling sends a unique list of latitude and longitude for sampling.
    * - use coordinates from a row key file, e.g. DR row key file created during 'load'. (-dr dr1)
    * - use coordinates from list of DRs. (-dr dr1,dr2,dr3)
    * - use coordinates from all except some DRs. (-edr dr1,dr2,dr3)
    * - use all coordinates from loc table. (no -dr or -edr)
    *
    * @param workingDir
    * @param threads
    * @param keepFiles
    * @param loadOccOnly
    * @param sampleOnly
    * @param drs
    * @param skipDrs
    * @param _useFullScan
    * @param startTokenRangeIdx
    * @param layers
    */
  def sampleRecords(workingDir: String, threads: Int, keepFiles: Boolean, loadOccOnly: Boolean, sampleOnly: Boolean,
                    drs: Seq[String], skipDrs: Seq[String], _useFullScan: Boolean, startTokenRangeIdx: Int,
                    layers: Seq[String], allNodes: Boolean = false): Unit = {

    val start = System.currentTimeMillis()
    var lastLog = System.currentTimeMillis()

    //note this update count isn't thread safe, so its inaccurate
    //its been left in to give a general idea of performance
    var updateCount = 0
    var readCount = 0
    var rowkeys = Seq("")
    val queue = new util.HashSet[String]()
    val dlat = "decimalLatitude" + Config.persistenceManager.fieldDelimiter + "p"
    val dlon = "decimalLongitude" + Config.persistenceManager.fieldDelimiter + "p"

    val rowKeyFile : File = if (drs.size == 1) {
      Store.rowKeyFile(drs.iterator.next())
    } else {
      null
    }

    // do not use full scan when no -dr or -edr
    val useFullScan = !(skipDrs.isEmpty && drs.isEmpty) && _useFullScan

    if (rowKeyFile != null && rowKeyFile.exists()) {
      println("Using rowKeyFile " + rowKeyFile.getPath)
      rowkeys = scala.io.Source.fromFile(rowKeyFile, "UTF-8").getLines().toSeq

      Config.persistenceManager.selectRows(rowkeys, "occ", Array(dlat, dlon), (map) => {
        readCount += 1

        val lat = map.getOrElse(dlat, "")
        val lon = map.getOrElse(dlon, "")
        if (lat != "" && lon != "") {
          queue.add(lon + "," + lat + "\n")
        }

        if (readCount % 1000 == 0) {
          logger.info("record coordinates read: " + readCount)
        }

        true
      })
      queue.size()
    } else if (useFullScan) {
      val _rowkeys = new util.ArrayList[String]()

      logger.info("Using a full scan...")
      Config.persistenceManager.asInstanceOf[Cassandra3PersistenceManager].pageOverLocalNotAsync("occ", (rowkey, map, _) => {
        val dr = map.getOrElse("dataResourceUid", "")
        if (!dr.isEmpty) {
          readCount += 1
          if ((drs.isEmpty || drs.contains(dr)) && !skipDrs.contains(dr)) {
            val lat = map.getOrElse(dlat, "")
            val lon = map.getOrElse(dlon, "")
            if (lat != "" && lon != "") {
              _rowkeys.add(rowkey)
              queue.add(lon + "," + lat + "\n")
            }

            updateCount += 1
          }
          if (updateCount % 10000 == 0) {
            val end = System.currentTimeMillis()
            val timeInSecs = ((end - lastLog).toFloat / 10000f)
            val recordsPerSec = Math.round(10000f / timeInSecs)
            logger.info(s"Total processed : $updateCount, total read: $readCount Last 1000 in $timeInSecs seconds ($recordsPerSec records a second)")
            lastLog = end
            ZookeeperUtil.setStatus("SAMPING", "RUNNING", updateCount)
          }
        }
        true
      }, threads, Array("dataResourceUid", dlat, dlon), localOnly = !allNodes)
      rowkeys = JavaConverters.asScalaIterableConverter(_rowkeys).asScala.toSeq
    } else if (drs.length > 0) {
      val _rowkeys = new util.ArrayList[String]()
      logger.info("Using a sequential scan by data resource...")
      var total = 0
      drs.foreach { dataResourceUid =>
        logger.info(s"Using a sequential scan by data resource...starting $dataResourceUid")
        Config.persistenceManager.asInstanceOf[Cassandra3PersistenceManager].pageOverLocalNotAsync("occ", (rowkey, map, _) => {
          readCount += 1
          val lat = map.getOrElse(dlat, "")
          val lon = map.getOrElse(dlon, "")
          if (lat != "" && lon != "") {
            _rowkeys.add(rowkey)
            queue.add(lon + "," + lat + "\n")
            updateCount += 1
          }

          if (updateCount % 100 == 0) {
            val end = System.currentTimeMillis()
            val timeInSecs = ((end - lastLog).toFloat / 10000f)
            val recordsPerSec = Math.round(10000f / timeInSecs)
            logger.info(s"Total processed : $updateCount, updateCounttotal read: $readCount Last 1000 in $timeInSecs seconds ($recordsPerSec records a second)")
            lastLog = end
            ZookeeperUtil.setStatus("SAMPING", "RUNNING", updateCount)
          }
          true
        }, threads, Array(dlat, dlon), localOnly = !allNodes, indexedField = "dataResourceUid", indexedFieldValue = dataResourceUid)
      }
      rowkeys = JavaConverters.asScalaIterableConverter(_rowkeys).asScala.toSeq
    } else if (!loadOccOnly) {
      // --load-occ-only does not require a queue of lat lon
      // --sampling-only does require a queue of lat lon but does not require a list of row keys
//      Config.persistenceManager.asInstanceOf[Cassandra3PersistenceManager].pageOverLocalNotAsync("loc", (key, map, _) => {
//        //rowkey is lon,lat
//        val rowkey = map.getOrElse("rowkey", "")
//        if (rowkey.length > 0) {
//          val latlon = rowkey.split("\\|")
//          if (latlon.length == 2) {
//            queue.add(latlon(1) + "," + latlon(0) +"\n")
//          }
//        }
//        true
//      }, threads, Array("rowkey"), localOnly = !allNodes)
      Config.persistenceManager.asInstanceOf[Cassandra3PersistenceManager].pageOverLocalNotAsync("occ", (key, map, _) => {
        //rowkey is lon,lat
        val lon = map.getOrElse(dlon, "")
        val lat = map.getOrElse(dlat, "")
        if (lat != "" && lon != "") {
          queue.add(lon + "," + lat + "\n")
          updateCount += 1
        }
        readCount += 1

        if (updateCount % 10000 == 0) {
          val end = System.currentTimeMillis()
          val timeInSecs = ((end - lastLog).toFloat / 10000f)
          val recordsPerSec = Math.round(10000f / timeInSecs)
          logger.info(s"Total processed : $updateCount, total read: $readCount Last 1000 in $timeInSecs seconds ($recordsPerSec records a second)")
          lastLog = end
          ZookeeperUtil.setStatus("SAMPING", "RUNNING", updateCount)
        }

        true
      }, threads, Array("rowkey", dlat, dlon), localOnly = !allNodes)
    }

    logger.info(s"found ${queue.size} unique coordinates for sampling")
    if (rowkeys.size > 0) {
      logger.info(s"found ${rowkeys.size} rowkeys for sampling")
    }
    sample(workingDir, threads, keepFiles, loadOccOnly, sampleOnly, queue, rowkeys, layers, allNodes)
  }

  def loadSamplingIntoOccurrences(threads: Int, rowkeys: Seq[String], allNodes: Boolean): Unit = {
    if (rowkeys.length > 0 && !rowkeys.iterator.next().isEmpty) {
      logger.info(s"Starting loading sampling for ${rowkeys.length} records")
    } else {
      logger.info(s"Starting loading sampling for all local=${!allNodes} records")
    }
    val dlat = "decimalLatitude" + Config.persistenceManager.fieldDelimiter + "p"
    val dlon = "decimalLongitude" + Config.persistenceManager.fieldDelimiter + "p"

    if (rowkeys.length > 0 && !rowkeys.iterator.next().isEmpty) {
      Config.persistenceManager.selectRows(rowkeys, "occ", Seq("rowkey", dlat, dlon), (map) => {
        val lat = map.getOrElse(dlat, "")
        val lon = map.getOrElse(dlon, "")
        val guid = map.getOrElse("rowkey", "")
        if (lat != "" && lon != "" && lat != null && lon != null && lat != "null" && lon != "null") {
          val point = LocationDAO.getSamplesForLatLon(lat, lon)
          if (!point.isEmpty) {
            val (location, environmentalLayers, contextualLayers) = point.get
            Config.persistenceManager.put(guid, "occ", Map(
              "el" + Config.persistenceManager.fieldDelimiter + "p" -> environmentalLayers,
              "cl" + Config.persistenceManager.fieldDelimiter + "p" -> contextualLayers),
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
      })
    } else {
      Config.persistenceManager.asInstanceOf[Cassandra3PersistenceManager].pageOverLocalNotAsync("occ", (guid, map, _) => {
        val lat = map.getOrElse(dlat, "")
        val lon = map.getOrElse(dlon, "")
        if (lat != "" && lon != "" && lat != null && lon != null && lat != "null" && lon != "null") {
          val point = LocationDAO.getSamplesForLatLon(lat, lon)
          if (!point.isEmpty) {
            val (location, environmentalLayers, contextualLayers) = point.get
            Config.persistenceManager.put(guid, "occ", Map(
//              "el" + Config.persistenceManager.fieldDelimiter + "p" -> Json.toJSON(environmentalLayers),
//              "cl" + Config.persistenceManager.fieldDelimiter + "p" -> Json.toJSON(contextualLayers)),
              "el" + Config.persistenceManager.fieldDelimiter + "p" -> environmentalLayers,
              "cl" + Config.persistenceManager.fieldDelimiter + "p" -> contextualLayers),
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
      }, threads, Array("rowkey", dlat, dlon), localOnly = !allNodes)
    }
  }
}
