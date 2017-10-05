package au.org.ala.biocache.tool

import java.io._
import java.util.concurrent.LinkedBlockingQueue

import au.com.bytecode.opencsv.{CSVReader, CSVWriter}
import au.org.ala.biocache.Config
import au.org.ala.biocache.Store.rowKeyFile
import au.org.ala.biocache.caches.LocationDAO
import au.org.ala.biocache.cmd.{IncrementalTool, Tool}
import au.org.ala.biocache.index.BulkProcessor.{counter, _}
import au.org.ala.biocache.model.QualityAssertion
import au.org.ala.biocache.processor.LocationProcessor
import au.org.ala.biocache.util.{FileHelper, Json, LayersStore, OptionParser}
import au.org.ala.layers.dao.IntersectCallback
import au.org.ala.layers.dto.IntersectionFile
import org.apache.commons.lang.StringUtils
import org.eclipse.jetty.util.ConcurrentHashSet
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashSet}

/**
  * Executable for running the sampling for a data resource.
  */
object Sampling extends Tool with IncrementalTool {

  def cmd = "sample"

  def desc = "Sample coordinates against geospatial layers"

  protected val logger = LoggerFactory.getLogger("Sampling")

  def main(args: Array[String]) {

    var dataResourceUid = ""
    var locFilePath = ""
    var singleLayerName = ""
    var rowKeyFile = ""
    var keepFiles = false
    var singleRowKey = ""
    var workingDir = Config.tmpWorkDir
    var batchSize = 100000
    var checkRowKeyFile = true
    var abortIfNotRowKeyFile = true
    var numThreads = 8

    val parser = new OptionParser(help) {
      opt("dr", "data-resource-uid", "the data resource to sample", {
        v: String => dataResourceUid = v
      })
      opt("cf", "coordinates-file", "the file containing coordinates", {
        v: String => locFilePath = v
      })
      opt("l", "single-layer-sample", "sample a single layer only", {
        v: String => singleLayerName = v
      })
      opt("rf", "row-key-file", "The row keys which to sample", {
        v: String => rowKeyFile = v
      })
      opt("keep", "Keep the files produced from the sampling", {
        keepFiles = true
      })
      opt("rk", "key", "the single rowkey to sample", {
        v: String => singleRowKey = v
      })
      opt("wd", "working-dir", "the directory to write temporary files too. Defaults to " + Config.tmpWorkDir, {
        v: String => workingDir = v
      })
      intOpt("bs", "batch-size", "Batch size when processing points. Defaults to " + batchSize, {
        v: Int => batchSize = v
      })
      opt("crk", "check for row key file", {
        checkRowKeyFile = true
      })
      opt("acrk", "abort if no row key file found", {
        abortIfNotRowKeyFile = true
      })
      intOpt("t", "threads", "The number of threads for the unique coordinate extract. The default is " + numThreads, {
        v: Int => numThreads = v
      })
    }

    if (parser.parse(args)) {
      val s = new Sampling

      if (dataResourceUid != "" && checkRowKeyFile) {
        val (hasRowKey, retrievedRowKeyFile) = ProcessRecords.hasRowKey(dataResourceUid)
        rowKeyFile = retrievedRowKeyFile.getOrElse("")
      }

      if (abortIfNotRowKeyFile && (rowKeyFile == "" || !(new File(rowKeyFile).exists()))) {
        logger.warn("No rowkey file was found for this sampling. Aborting.")
      } else {
        //for this data resource
        val fileSuffix = {
          if (dataResourceUid != "") {
            logger.info("Sampling : " + dataResourceUid)
            dataResourceUid
          } else {
            logger.info("Sampling all records")
            "all"
          }
        }

        if (locFilePath == "") {
          locFilePath = workingDir + "/loc-" + fileSuffix + ".txt"
          if (rowKeyFile == "" && singleRowKey == "") {
            s.getDistinctCoordinatesForResourceThreaded(numThreads, locFilePath, dataResourceUid)
          } else if (singleRowKey != "") {
            s.getDistinctCoordinatesForRowKey(singleRowKey)
            sys.exit
          } else {
            s.getDistinctCoordinatesForFile(locFilePath, rowKeyFile)
          }
        }

        val samplingFilePath = workingDir + "/sampling-" + fileSuffix + ".txt"
        //generate sampling
        s.sampling(locFilePath,
          samplingFilePath,
          batchSize = batchSize,
          concurrentLoading = true,
          keepFiles = keepFiles,
          layers = Array(singleLayerName)
        )

        //load sampling to occurrence records
        logger.info("Loading sampling into occ table")
        if (dataResourceUid != null) {
          loadSamplingIntoOccurrences(dataResourceUid)
        }
        logger.info("Completed loading sampling into occ table")

        //clean up the file
        if (!keepFiles) {
          logger.info(s"Removing temporary file: $samplingFilePath")
          (new File(samplingFilePath)).delete()
          if (new File(locFilePath).exists()) (new File(locFilePath)).delete()
        }
      }
    }
  }

  /**
    * Loads the sampling into the occ table.
    * This single threaded and slow.
    */
  def loadSamplingIntoOccurrences(dataResourceUid: String): Unit = {
    logger.info(s"Starting loading sampling for $dataResourceUid")
    Config.persistenceManager.pageOverSelect("occ", (guid, map) => {
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
    }, "dataResourceUid", dataResourceUid, 1000, "decimalLatitude" + Config.persistenceManager.fieldDelimiter + "p", "decimalLongitude" + Config.persistenceManager.fieldDelimiter + "p")
  }

  def sampleDataResource(dataResourceUid: String, callback: IntersectCallback = null, singleLayerName: String = "") {
    val locFilePath = Config.tmpWorkDir + "/loc-" + dataResourceUid + ".txt"
    val s = new Sampling
    s.getDistinctCoordinatesForResourceThreaded(4, locFilePath, dataResourceUid)
    val samplingFilePath = Config.tmpWorkDir + "/sampling-" + dataResourceUid + ".txt"
    //generate sampling
    s.sampling(locFilePath, samplingFilePath, callback, layers = Array(singleLayerName))
    //load the loc table
    s.loadSampling(samplingFilePath, callback)
    //clean up the file
    logger.info("Removing temporary file: " + samplingFilePath)
    (new File(samplingFilePath)).delete()
    (new File(locFilePath)).delete()
  }
}

class PointsReader(filePath: String) {

  val logger = LoggerFactory.getLogger("PointsReader")
  val csvReader = new CSVReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"))

  /**
    * Load a set of points from a CSV file
    */
  def loadPoints(max: Int): Array[Array[Double]] = {
    //load the CSV of points into memory
    logger.info("Loading points from file: " + filePath)
    var current: Array[String] = csvReader.readNext
    val points: ArrayBuffer[Array[Double]] = new ArrayBuffer[Array[Double]]
    var count = 0
    while (current != null && count < max) {
      try {
        points += current.map(x => x.toDouble)
      } catch {
        case e: Exception => logger.error("Error reading point: " + current)
      }
      count += 1
      current = csvReader.readNext
    }
    points.toArray
  }

  def close = csvReader.close
}

class Sampling {

  val logger = LoggerFactory.getLogger("Sampling")

  import FileHelper._

  def handleLatLongInMap(map: Map[String, String], coordinates: mutable.HashSet[String], lp: LocationProcessor) {
    val latLongWithOption = lp.processLatLong(map.getOrElse("decimalLatitude", null),
      map.getOrElse("decimalLongitude", null),
      map.getOrElse("geodeticDatum", null),
      map.getOrElse("verbatimLongitude", null),
      map.getOrElse("verbatimLongitude", null),
      map.getOrElse("verbatimSRS", null),
      map.getOrElse("easting", null),
      map.getOrElse("northing", null),
      map.getOrElse("zone", null),
      map.getOrElse("gridReference", null),
      new ArrayBuffer[QualityAssertion]
    )
    latLongWithOption match {
      case Some(latLong) => {
        coordinates += (latLong.longitude + "," + latLong.latitude) // write long lat (x,y)
        coordinates += (latLong.longitude.toFloat.toString.trim + "," + latLong.latitude.toFloat.toString.trim)
      }
      case None => {}
    }
  }

  def handleRecordMap(map: Map[String, String], coordinates: HashSet[String], lp: LocationProcessor) {
    handleLatLongInMap(map, coordinates, lp)

    val originalSensitiveValues = map.getOrElse("originalSensitiveValues", "")
    if (originalSensitiveValues != "") {
      val sensitiveLatLong = Json.toMap(originalSensitiveValues)
      val lat = sensitiveLatLong.getOrElse("decimalLatitude", null)
      val lon = sensitiveLatLong.getOrElse("decimalLongitude", null)
      if (lat != null && lon != null) {
        coordinates += (lon + "," + lat)
        val newMap = map ++ Map("decimalLatitude" -> lat.toString, "decimalLongitude" -> lon.toString)
        handleLatLongInMap(newMap, coordinates, lp)
      }
    }

    //legacy storage of old lat/long original values before SDS processing - superceded by originalSensitiveValues
    val originalDecimalLatitude = map.getOrElse("originalDecimalLatitude", "")
    val originalDecimalLongitude = map.getOrElse("originalDecimalLongitude", "")
    if (originalDecimalLatitude != "" && originalDecimalLongitude != "") {
      coordinates += (originalDecimalLongitude + "," + originalDecimalLatitude)
    }

    //add the processed values
    val processedDecimalLatitude = map.getOrElse("decimalLatitude" + Config.persistenceManager.fieldDelimiter + "p", "")
    val processedDecimalLongitude = map.getOrElse("decimalLongitude" + Config.persistenceManager.fieldDelimiter + "p", "")
    if (processedDecimalLatitude != "" && processedDecimalLongitude != "") {
      coordinates += (processedDecimalLongitude + "," + processedDecimalLatitude)
    }
  }

  val properties = Array(
    "decimalLatitude", "decimalLongitude",
    "decimalLatitude" + Config.persistenceManager.fieldDelimiter + "p", "decimalLongitude" + Config.persistenceManager.fieldDelimiter + "p",
    "verbatimLatitude", "verbatimLongitude",
    "originalDecimalLatitude", "originalDecimalLongitude", "originalSensitiveValues",
    "geodeticDatum", "verbatimSRS", "easting", "northing", "zone")

  def getDistinctCoordinatesForRowKey(rowKey: String) {
    val values = Config.persistenceManager.getSelected(rowKey, "occ", properties)
    if (values.isDefined) {
      val coordinates = new HashSet[String]
      handleRecordMap(values.get, coordinates, new LocationProcessor)
      println(coordinates)
    }
  }

  def getDistinctCoordinatesForFile(locFilePath: String, rowKeyFile: String) {
    logger.info(s"Creating distinct list of coordinates for row keys in $rowKeyFile")
    var counter = 0
    var passed = 0
    val rowKeys = new File(rowKeyFile)
    val coordinates = new HashSet[String]
    val lp = new LocationProcessor
    rowKeys.foreachLine { line =>
      val values = Config.persistenceManager.getSelected(line, "occ", properties)
      if (values.isDefined) {
        def map = values.get

        handleRecordMap(map, coordinates, lp)

        if (counter % 10000 == 0 && counter > 0) {
          val numberOfCoordinates = coordinates.size
          logger.debug(s"Distinct coordinates counter: $counter , current count: $numberOfCoordinates")
        }
        counter += 1
        passed += 1
      }
    }

    try {
      val fw = new FileWriter(locFilePath)
      coordinates.foreach { c =>
        fw.write(c)
        fw.write("\n")
      }
      fw.flush
      fw.close
    } catch {
      case e: Exception => logger.error("Failed to write - " + e.getMessage, e)
    }
  }

  /**
    * Get the distinct coordinates for this resource
    * and write them to file.
    */
  def getDistinctCoordinatesForResourceThreaded(numThreads: Int, locFilePath: String, dataResourceUid: String = "") {

    logger.info("Creating distinct list of coordinates....")

    val lce = new LocColumnExporter(counter, dataResourceUid, handleRecordMap)
    lce.run
    try {
      val fw = new FileWriter(locFilePath)
      lce.coordinates.foreach(c => {
        fw.write(c)
        fw.write("\n")
      })
      fw.flush
      fw.close
    } catch {
      case e: Exception => logger.error(e.getMessage, e)
    }
  }

  /**
    * Run the sampling with a file
    */
  def sampling(filePath: String, outputFilePath: String, callback: IntersectCallback = null,
               batchSize: Int = 100000, concurrentLoading: Boolean = false,
               keepFiles: Boolean = true, layers: Seq[String]) {

    logger.info("********* START - TEST BATCH SAMPLING FROM FILE ***************")
    //load the CSV of points into memory
    val pointsReader = new PointsReader(filePath)
    val fields = if (layers.nonEmpty) {
      layers.toArray
    } else {
      Config.fieldsToSample()
    }
    var filename = outputFilePath
    var writer = new CSVWriter(new FileWriter(filename))
    val batch = new LinkedBlockingQueue[String]
    var batchCount = 0
    val sampleLoading = new LoadSamplingConsumer(batch)
    if (concurrentLoading) {
      sampleLoading.start()
    }

    //write the header
    writer.writeNext(Array("longitude", "latitude") ++ fields)
    var totalProcessed = 0
    var points = pointsReader.loadPoints(batchSize)
    while (!points.isEmpty) {

      //do the sampling
      if (Config.layersServiceSampling) {
        processBatchRemote(writer, points, fields, callback)
      } else {
        processBatch(writer, points, fields, callback)
      }

      totalProcessed += points.size
      logger.info("Total points sampled so far : " + totalProcessed)
      //read next batch
      points = pointsReader.loadPoints(batchSize)

      if (concurrentLoading) {
        batchCount += 1
        batch.put(filename)

        //close current and open new file for loading
        writer.flush()
        writer.close()

        filename = outputFilePath + totalProcessed
        writer = new CSVWriter(new FileWriter(filename))

        //write the header
        writer.writeNext(Array("longitude", "latitude") ++ fields)
      }
    }
    pointsReader.close
    writer.flush()
    writer.close()

    logger.info("Total points sampled : " + totalProcessed + ", output file: " + outputFilePath + " point file: " + filePath)
    logger.info("********* END - TEST BATCH SAMPLING FROM FILE ***************")

    if (concurrentLoading) {
      //wait for loading to finish
      while (sampleLoading.doneList.size() < batchCount) {
        Thread.sleep(50)
      }
      //terminate thread
      try {
        sampleLoading.interrupt()
      }
      //delete files
      if (!keepFiles) {
        sampleLoading.doneList.toArray.foreach(f => {
          new File(f.toString).delete()
        })
      }
    }
  }

  private def processBatch(writer: CSVWriter, points: Array[Array[Double]], fields: Array[String], callback: IntersectCallback = null): Unit = {

    //process a batch of points
    val layerIntersectDAO = au.org.ala.layers.client.Client.getLayerIntersectDao()

    //perform the sampling
    val samples: java.util.ArrayList[String] = layerIntersectDAO.sampling(fields, points, callback)

    val columns: Array[Array[String]] = Array.ofDim(samples.size, points.length)

    for (i <- 0 until samples.size) {
      columns(i) = samples.get(i).split('\n')
    }

    for (i <- 0 until points.length) {
      val sampledPoint = Array.fill(2 + columns.length)("")
      sampledPoint(0) = points(i)(0).toString()
      sampledPoint(1) = points(i)(1).toString()
      for (j <- 0 until columns.length) {
        if (i < columns(j).length) {
          if (columns(j)(i) != "n/a") {
            sampledPoint(j + 2) = columns(j)(i)
          }
        }
      }
      writer.writeNext(sampledPoint.toArray)
      writer.flush
    }
  }

  /**
    * Remote sampling
    */
  private def processBatchRemote(writer: CSVWriter, points: Array[Array[Double]], fields: Array[String], callback: IntersectCallback = null): Unit = {

    def layersStore = new LayersStore(Config.layersServiceUrl)

    //callback setup
    if (callback != null) {
      //dummy info
      val intersectionFiles: Array[IntersectionFile] = new Array[IntersectionFile](fields.length)
      for (j <- 0 until fields.length) {
        intersectionFiles(j) = new IntersectionFile(fields(j), "", "", "layer " + (j + 1), "", "", "", "", null)
      }
      callback.setLayersToSample(intersectionFiles)
    }
    //do sampling
    val samples = new CSVReader(layersStore.sample(fields, points, callback))

    //header
    var header = samples.readNext()

    //write sampling
    var row = samples.readNext()
    var rowCounter: Integer = 0
    while (row != null) {
      if (callback != null && rowCounter % 500 == 0) {
        callback.setCurrentLayer(new IntersectionFile("", "", "", "finished. Loaded " + rowCounter, "", "", "", "", null))
        callback.progressMessage("Loading sampling.")

      }
      rowCounter += 1

      //swap longitude and latitude
      val lng = row(0)
      row(0) = row(1)
      row(1) = lng

      writer.writeNext(row)
      row = samples.readNext()
    }

    writer.flush()

    if (callback != null) {
      callback.setCurrentLayer(new IntersectionFile("", "", "", "finished. Load sampling finished", "", "", "", "", null))
      callback.progressMessage("Load sampling finished.")
    }
  }

  /**
    * Load the sampling into the loc table
    */
  def loadSampling(inputFileName: String, callback: IntersectCallback = null) {

    logger.info("Loading the sampling into the database")

    val startTime = System.currentTimeMillis
    var nextTime = System.currentTimeMillis
    try {
      val csvReader = new CSVReader(new InputStreamReader(new FileInputStream(inputFileName), "UTF-8"))
      val header = csvReader.readNext
      var counter = 0
      var line = csvReader.readNext

      val batches = new scala.collection.mutable.HashMap[String, Map[String, String]]
      val batchSize = 500
      while (line != null) {
        try {
          val map = (header zip line).filter(x => !StringUtils.isEmpty(x._2.trim) && x._1 != "latitude" && x._1 != "longitude").toMap
          val el = map.filter(x => x._1.startsWith("el") && x._2 != "n/a").map(y => {
            y._1 -> y._2.toFloat
          })
          val cl = map.filter(x => x._1.startsWith("cl") && x._2 != "n/a").toMap
          if (batches.size == batchSize) {
            LocationDAO.writeLocBatch(batches)
            batches.clear()
          }
          batches += LocationDAO.addLayerIntersects(line(1), line(0), cl, el, true)
          if (counter % 200 == 0 && callback != null) {
            callback.setCurrentLayer(new IntersectionFile("", "", "", "finished. Processing loaded samples " + counter, "", "", "", "", null))
            callback.progressMessage("Loading sampling.")
          }
          if (counter % 1000 == 0) {
            logger.info(s"writing to loc: $counter : records per sec: " + 1000f / (((System.currentTimeMillis - nextTime).toFloat) / 1000f))
            nextTime = System.currentTimeMillis
          }
          counter += 1
        } catch {
          case e: Exception => {
            logger.error(e.getMessage, e)
            logger.error("Problem writing line: " + counter + ", line length: " + line.length + ", header length: " + header.length)
          }
        }
        line = csvReader.readNext
      }
      csvReader.close
      if (batches.size > 0) {
        LocationDAO.writeLocBatch(batches.toMap)
      }
    } catch {
      case e: Exception => {
        logger.error("Problem loading sampling. " + e.getMessage, e)
      }
    }
    logger.info("Finished loading: " + inputFileName + " in " + (System.currentTimeMillis - startTime) + "ms")
  }
}

/**
  * A location coordinates set builder that can be used in a threaded manner
  *
  * @param threadId
  */
class LocColumnExporter(threadId: Int, dataResourceUid: String, handleRecordMap: (Map[String, String], HashSet[String], LocationProcessor) => Unit) extends Thread {

  val logger = LoggerFactory.getLogger("LocColumnExporter")

  val coordinates = new HashSet[String]

  import FileHelper._


  override def run {
    try {

      logger.info("unique coordinates start thread: " + threadId)

      val start = System.currentTimeMillis
      var counter = 0
      val lp = new LocationProcessor

      val keyFile = rowKeyFile(dataResourceUid)
      if (keyFile.exists()) {
        logger.info("Using rowKeyFile " + keyFile.getPath)
        keyFile.foreachLine(line => {
          val map = Config.persistenceManager.getSelected(line, "occ", Array("decimalLatitude",
            "decimalLongitude",
            "decimalLatitude" + Config.persistenceManager.fieldDelimiter + "p",
            "decimalLongitude" + Config.persistenceManager.fieldDelimiter + "p",
            "verbatimLatitude",
            "verbatimLongitude",
            "originalDecimalLatitude",
            "originalDecimalLongitude",
            "originalSensitiveValues"))
          handleRecordMap(map.get, coordinates, lp)

          if (counter % 100000 == 0 && counter > 0) {
            logger.info(s"records counter: $counter thread: $threadId, unique count: $coordinates.size")
          }
          counter += 1
          Integer.MAX_VALUE > counter
        })
      } else {
        logger.info("Using query for dataResourceUid")
        val (field, value) = if (StringUtils.isNotEmpty(dataResourceUid)) {
          ("dataResourceUid", dataResourceUid)
        } else {
          ("", "")
        }

        Config.persistenceManager.pageOverSelect("occ", (uuid, map) => {
          handleRecordMap(map, coordinates, lp)

          if (counter % 100000 == 0 && counter > 0) {
            logger.info(s"records counter: $counter thread: $threadId, unique count: $coordinates.size")
          }
          counter += 1
          Integer.MAX_VALUE > counter

        }, field, value, 1000, "decimalLatitude",
          "decimalLongitude",
          "decimalLatitude" + Config.persistenceManager.fieldDelimiter + "p",
          "decimalLongitude" + Config.persistenceManager.fieldDelimiter + "p",
          "verbatimLatitude",
          "verbatimLongitude",
          "originalDecimalLatitude",
          "originalDecimalLongitude",
          "originalSensitiveValues")
      }

      val fin = System.currentTimeMillis
      logger.info("[Unique Coordinate Thread " + threadId + "] " + counter + " took " + ((fin - start).toFloat) / 1000f + " seconds")
    } catch {
      case e: Exception => logger.error("Error reading points for thread: " + threadId, e)
    }
  }
}

class LoadSamplingConsumer(batches: LinkedBlockingQueue[String]) extends Thread {

  val doneList = new ConcurrentHashSet[String]

  /**
    * Load the sampling into the loc table
    */
  override def run() {

    try {

      while (true) {
        val inputFileName = batches.take()

        logger.info("Loading the sampling into the database: " + inputFileName)

        val startTime = System.currentTimeMillis
        var nextTime = System.currentTimeMillis
        try {
          val csvReader = new CSVReader(new InputStreamReader(new FileInputStream(inputFileName), "UTF-8"))
          val header = csvReader.readNext
          var counter = 0
          var line = csvReader.readNext

          val batches = new scala.collection.mutable.HashMap[String, Map[String, String]]
          val batchSize = 500
          while (line != null) {
            try {
              val map = (header zip line).filter(x => !StringUtils.isEmpty(x._2.trim) && x._1 != "latitude" && x._1 != "longitude").toMap
              val el = map.filter(x => x._1.startsWith("el") && x._2 != "n/a").map(y => {
                y._1 -> y._2.toFloat
              }).toMap
              val cl = map.filter(x => x._1.startsWith("cl") && x._2 != "n/a").toMap
              if (batches.size == batchSize) {
                LocationDAO.writeLocBatch(batches.toMap)
                batches.clear()
              }
              batches += LocationDAO.addLayerIntersects(line(1), line(0), cl, el, true)

              if (counter % 1000 == 0 && counter > 0) {
                logger.info("writing to loc:" + counter + ": records per sec: " + 1000f / (((System.currentTimeMillis - nextTime).toFloat) / 1000f))
                nextTime = System.currentTimeMillis
              }
              counter += 1
            } catch {
              case e: Exception => {
                logger.error(e.getMessage, e)
                logger.error("Problem writing line: " + counter + ", line length: " + line.length + ", header length: " + header.length)
              }
            }
            line = csvReader.readNext
          }
          csvReader.close
          if (batches.size > 0) {
            LocationDAO.writeLocBatch(batches.toMap)
          }
        } catch {
          case e: Exception => {
            logger.error("Problem loading sampling. " + e.getMessage, e)
          }
        }
        logger.info("Finished loading: " + inputFileName + " in " + (System.currentTimeMillis - startTime) + "ms")

        doneList.add(inputFileName)
      }
    } catch {
      case e: InterruptedException => {
        //InterruptedException is expected when the thread is terminated with an interruption.
      }
    }
  }
}
