package au.org.ala.biocache.tool

import java.io._
import org.ala.layers.dto.IntersectionFile
import org.apache.commons.lang.StringUtils
import au.org.ala.biocache._
import au.com.bytecode.opencsv.{CSVWriter, CSVReader}
import scala.collection.mutable.{ArrayBuffer, HashSet}
import org.slf4j.LoggerFactory
import org.ala.layers.dao.IntersectCallback
import collection.mutable
import au.org.ala.biocache.processor.LocationProcessor
import au.org.ala.biocache.caches.LocationDAO
import au.org.ala.biocache.model.QualityAssertion
import au.org.ala.biocache.util.{LayersStore, OptionParser, Json, FileHelper}
import au.org.ala.biocache.cmd.{IncrementalTool, Tool}

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
    var checkRowKeyFile = false
    var abortIfNotRowKeyFile = false

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
      opt("keep", "Keep the files produced from the sampling",{
        keepFiles = true
      })
      opt("rk","key", "the single rowkey to sample",{
        v:String => singleRowKey = v
      })
      opt("wd","working-dir", "the directory to write temporary files too. Defaults to " +  Config.tmpWorkDir,{
        v:String => workingDir = v
      })
      intOpt("bs","batch-size", "Batch size when processing points. Defaults to " + batchSize, {
        v:Int => batchSize = v
      })
      opt("crk", "check for row key file", { checkRowKeyFile = true })
      opt("acrk", "abort if no row key file found",{ abortIfNotRowKeyFile = true })
    }

    if (parser.parse(args)) {
      val s = new Sampling

      if(dataResourceUid != "" && checkRowKeyFile){
        val (hasRowKey, retrievedRowKeyFile) = ProcessRecords.hasRowKey(dataResourceUid)
        rowKeyFile = retrievedRowKeyFile.getOrElse("")
      }

      if(abortIfNotRowKeyFile && (rowKeyFile=="" || !(new File(rowKeyFile).exists()))) {
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
            s.getDistinctCoordinatesForResource(locFilePath, dataResourceUid)
          } else if (singleRowKey != "") {
            s.getDistinctCoordinatesForRowKey(singleRowKey)
            sys.exit
          } else {
            s.getDistinctCoordinatesForFile(locFilePath, rowKeyFile)
          }
        }

        val samplingFilePath = workingDir + "/sampling-" + fileSuffix + ".txt"
        //generate sampling
        s.sampling(locFilePath, samplingFilePath, singleLayerName=singleLayerName, batchSize=batchSize)
        //load the loc table
        s.loadSampling(samplingFilePath)
        //clean up the file
        if(!keepFiles){
          logger.info("Removing temporary file: " + samplingFilePath)
          (new File(samplingFilePath)).delete()
          (new File(locFilePath)).delete()
        }
      }
    }
  }

  def sampleDataResource(dataResourceUid: String, callback:IntersectCallback = null, singleLayerName: String = "") {
    val locFilePath = Config.tmpWorkDir + "/loc-" + dataResourceUid + ".txt"
    val s = new Sampling
    s.getDistinctCoordinatesForResource(locFilePath, dataResourceUid)
    val samplingFilePath = Config.tmpWorkDir + "/sampling-" + dataResourceUid + ".txt"
    //generate sampling
    s.sampling(locFilePath, samplingFilePath, callback, singleLayerName)
    //load the loc table
    s.loadSampling(samplingFilePath, callback)
    //clean up the file
    logger.info("Removing temporary file: " + samplingFilePath)
    (new File(samplingFilePath)).delete()
    (new File(locFilePath)).delete()
  }
}

class PointsReader(filePath:String) {

  val logger = LoggerFactory.getLogger("PointsReader")
  val csvReader = new CSVReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"))

  /**
   * Load a set of points from a CSV file
   */
  def loadPoints(max:Int): Array[Array[Double]] = {
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

  def handleLatLongInMap(map:Map[String,String], coordinates:mutable.HashSet[String], lp:LocationProcessor){
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
        coordinates += (latLong.longitude.toFloat.toString.trim+ ","+latLong.latitude.toFloat.toString.trim)
      }
      case None => {}
    }
  }

  def handleRecordMap(map:Map[String,String], coordinates:HashSet[String], lp:LocationProcessor){
    handleLatLongInMap(map, coordinates, lp)

    val originalSensitiveValues = map.getOrElse("originalSensitiveValues", "")
    if (originalSensitiveValues != "") {
      val sensitiveLatLong = Json.toMap(originalSensitiveValues)
      val lat = sensitiveLatLong.getOrElse("decimalLatitude", null)
      val lon = sensitiveLatLong.getOrElse("decimalLongitude", null)
      if (lat != null && lon != null) {
        coordinates += (lon + "," + lat)
        val newMap = map ++ Map("decimalLatitude"-> lat.toString, "decimalLongitude"->lon.toString)
        handleLatLongInMap(newMap, coordinates,lp)
      }
    }

    //legacy storage of old lat/long original values before SDS processing - superceded by originalSensitiveValues
    val originalDecimalLatitude = map.getOrElse("originalDecimalLatitude", "")
    val originalDecimalLongitude = map.getOrElse("originalDecimalLongitude", "")
    if (originalDecimalLatitude != "" && originalDecimalLongitude != "") {
      coordinates += (originalDecimalLongitude + "," + originalDecimalLatitude)
    }

    //add the processed values
    val processedDecimalLatitude = map.getOrElse("decimalLatitude.p", "")
    val processedDecimalLongitude = map.getOrElse("decimalLongitude.p", "")
    if (processedDecimalLatitude != "" && processedDecimalLongitude != "") {
      coordinates += (processedDecimalLongitude + "," + processedDecimalLatitude)
    }
  }

  val properties = Array("decimalLatitude", "decimalLongitude",
    "decimalLatitude.p", "decimalLongitude.p",
    "verbatimLatitude", "verbatimLongitude",
    "originalDecimalLatitude", "originalDecimalLongitude",
    "originalSensitiveValues", "geodeticDatum", "verbatimSRS", "easting", "northing", "zone")

  def getDistinctCoordinatesForRowKey(rowKey:String){
    val values = Config.persistenceManager.getSelected(rowKey, "occ", properties)
    if(values.isDefined){
      val coordinates = new HashSet[String]
      handleRecordMap(values.get, coordinates, new LocationProcessor)
      println(coordinates)
    }
  }

  def getDistinctCoordinatesForFile(locFilePath: String, rowKeyFile: String) {
    logger.info("Creating distinct list of coordinates for row keys in " + rowKeyFile)
    var counter = 0
    var passed = 0
    val rowKeys = new File(rowKeyFile)
    val coordinates = new HashSet[String]
    val lp = new LocationProcessor
    rowKeys.foreachLine(line => {
      val values = Config.persistenceManager.getSelected(line, "occ", properties)
      if (values.isDefined) {
        def map = values.get
        handleRecordMap(map, coordinates, lp)

        if (counter % 10000 == 0 && counter > 0) {
          logger.debug("Distinct coordinates counter: " + counter + ", current count:" + coordinates.size)
        }
        counter += 1
        passed += 1
      }
    })

    try {
      val fw = new FileWriter(locFilePath)
      coordinates.foreach(c => {
        fw.write(c)
        fw.write("\n")
      })
      fw.flush
      fw.close
    } catch {
      case e:Exception =>  logger.error("failed to write - " + e.getMessage, e)
    }
  }

  /**
   * Get the distinct coordinates for this resource
   * and write them to file.
   */
  def getDistinctCoordinatesForResource(locFilePath: String, dataResourceUid: String = "") {
    logger.info("Creating distinct list of coordinates....")
    var counter = 0
    var passed = 0

    val startUuid: String = {
      if (dataResourceUid == "") ""
      else dataResourceUid + "|"
    }
    val endUuid: String = {
      if (dataResourceUid == "") ""
      else dataResourceUid + "|~"
    }

    val lp = new LocationProcessor
    val coordinates = new HashSet[String]

    Config.persistenceManager.pageOverSelect("occ", (guid, map) => {
      handleRecordMap(map, coordinates,lp)

      if (counter % 10000 == 0 && counter > 0){
        logger.debug("Distinct coordinates counter: " + counter + ", current count:" + coordinates.size)
      }
      counter += 1
      passed += 1
      Integer.MAX_VALUE > counter
    }, startUuid, endUuid, 1000, "decimalLatitude", "decimalLongitude","decimalLatitude.p", "decimalLongitude.p",
      "verbatimLatitude", "verbatimLongitude","originalDecimalLatitude", "originalDecimalLongitude",
      "originalSensitiveValues")

    try {
      val fw = new FileWriter(locFilePath)
      coordinates.foreach(c => {
        fw.write(c)
        fw.write("\n")
      })
      fw.flush
      fw.close
    } catch {
      case e:Exception =>  logger.error(e.getMessage,e)
    }
  }

  /**
   * Run the sampling with a file
   */
  def sampling(filePath: String, outputFilePath: String, callback:IntersectCallback = null,singleLayerName: String = "",batchSize:Int= 100000) {

    logger.info("********* START - TEST BATCH SAMPLING FROM FILE ***************")
    //load the CSV of points into memory
    val pointsReader = new PointsReader(filePath)
    val fields = if (singleLayerName != "") {
      Array(singleLayerName)
    } else {
      Config.fieldsToSample
    }
    val writer = new CSVWriter(new FileWriter(outputFilePath))
    //write the header
    writer.writeNext(Array("longitude", "latitude") ++ fields)
    var totalProcessed = 0
    var points = pointsReader.loadPoints(batchSize)
    while(!points.isEmpty) {
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
    }
    pointsReader.close
    writer.flush()
    writer.close()

    logger.info("Total points sampled : " + totalProcessed + ", output file: " + outputFilePath + " point file: " + filePath)
    logger.info("********* END - TEST BATCH SAMPLING FROM FILE ***************")
  }


  private def processBatch(writer: CSVWriter, points: Array[Array[Double]], fields: Array[String], callback:IntersectCallback=null): Unit = {

    //process a batch of points
    val layerIntersectDAO = org.ala.layers.client.Client.getLayerIntersectDao()

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

  /* remote sampling */
  private def processBatchRemote(writer: CSVWriter, points: Array[Array[Double]], fields: Array[String], callback:IntersectCallback=null): Unit = {

    def layersStore = new LayersStore(Config.layersServiceUrl)

    //callback setup
    if (callback != null) {
      //dummy info
      val intersectionFiles: Array[IntersectionFile] = new Array[IntersectionFile](fields.length)
      for (j <- 0 until fields.length) {
        intersectionFiles(j) = new IntersectionFile(fields(j),"","","layer " + (j + 1),"","","","",null)
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
        callback.setCurrentLayer(new IntersectionFile("","","","finished. Loaded " + rowCounter, "","","","",null))
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

      while (line != null) {
        try {
          val map = (header zip line).filter(x => !StringUtils.isEmpty(x._2.trim) && x._1 != "latitude" && x._1 != "longitude").toMap
          val el = map.filter(x => x._1.startsWith("el") && x._2 != "n/a").map(y => {
              y._1 -> y._2.toFloat
          }).toMap
          val cl = map.filter(x => x._1.startsWith("cl") && x._2 != "n/a").toMap
          LocationDAO.addLayerIntersects(line(1), line(0), cl, el)
          if (counter % 200 == 0 && callback != null) {
            callback.setCurrentLayer(new IntersectionFile("","","","finished. Processing loaded samples " + counter, "","","","",null))
            callback.progressMessage("Loading sampling.")
          }
          if (counter % 1000 == 0) {
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
    }
    logger.info("Finished loading: " + inputFileName + " in " + (System.currentTimeMillis - startTime) + "ms")
  }
}
