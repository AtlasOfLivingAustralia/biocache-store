package au.org.ala.biocache.export

import java.io._
import java.net.{URL, URLEncoder}
import java.util
import java.util.zip.{GZIPInputStream, GZIPOutputStream, ZipEntry, ZipOutputStream}

import au.com.bytecode.opencsv.{CSVReader, CSVWriter}
import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.index.{Counter}
import au.org.ala.biocache.util.{LayersStore, OptionParser}
import au.org.ala.biocache.vocab.{AssertionCodes, ErrorCode}
import org.apache.commons.io.FileUtils
import org.apache.log4j.lf5.util.StreamUtils
import org.json.simple.parser.JSONParser
import org.json.simple.{JSONArray, JSONObject}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.parsing.json.JSON

/**
  * Utility for exporting a list of fields from the index using SOLR streaming.
  */
object ExportFromIndexStream extends Tool with Counter {

  def cmd = "export-stream"

  def desc = "Export from search indexes using streaming."

  override val logger = LoggerFactory.getLogger("ExportFromIndexStream")

  var numThreads = 8
  var outputFilePath = ""
  var query = "*:*"
  var fieldsToExport = Array[String]()
  var orderFields = Array("row_key")
  var queryAssertions = false
  var cassandraFilterFile = ""

  var keys: Option[Array[String]] = None
  var dr: Option[String] = None

  //cassandraFilterFile fields
  var filters: JSONArray = null
  var outputFiles = mutable.ListBuffer[ArrayBuffer[CSVWriter]]()
  var filterList = mutable.ListBuffer[(String, String)]()
  var fieldsList = mutable.ListBuffer[(Array[String], Array[String], Array[Int], Boolean)]()
  var fieldsListBrief = mutable.ListBuffer[(Array[String], Array[String], Array[Int], Boolean)]()
  var fieldsListNormal = mutable.ListBuffer[(Array[String], Array[String], Array[Int], Boolean)]()

  def main(args: Array[String]) {
    val parser = new OptionParser(help) {
      argOpt("output-file", "The file name for the export file", {
        v: String => outputFilePath = v
      })
      argOpt("list-of-fields", "CSV list of fields to export", {
        v: String => fieldsToExport = v.split(",").toArray
      })
      opt("q", "query", "The SOLR query to use", {
        v: String => query = v
      })
      opt("qa", "quality-assertions", "Include query assertions", {
        queryAssertions = true
      })
      opt("f", "filter-file", "Cassandra filter file for export-stream. File is a json file with an array of maps with " +
        "the configuration of each export file. list-of-fields must contain all fields in -subset1 and " +
        "-subset2. 'cassandraField' can be 'species_group'. 'value' can be '*'. -subset1 and -subset2 can" +
        " include layer (el and/or cl) and qa field names when qa:true and layers:true. output-file value must end with '.csv'. " +
        " the optional -subset1 and -subset2 field lists will create output files with the name extensions -brief " +
        "and -standard" +
        "[{filter:\"cassandraField:value\", output-file:filepath, qa:false, " +
        "layers:false, user-qa:false, list-of-fields:id,scientificName" + Config.persistenceManager.fieldDelimiter + "p, list-of-fields-subset1:id, " +
        "list-of-fields-subset2:scientificName" + Config.persistenceManager.fieldDelimiter + "p metadata-q:\"species_group:Fishes\"}]", {
        v: String => cassandraFilterFile = v
      })
      opt("k", "keys", "A comma separated list of keys on which to perform the range threads. Prevents the need to query SOLR for the ranges. Only used with a filter-file. ", {
        v: String => keys = Some(v.split(","))
      })
      opt("dr", "dr", "The data resource over which to obtain the range. Only used with a filter-file. ", {
        v: String => dr = Some(v)
      })
      intOpt("t", "threads", "The number of threads to perform the indexing on. Only used with a filter-file. Default is " + numThreads, {
        v: Int => numThreads = v
      })
    }

    if (parser.parse(args)) {
      if (cassandraFilterFile.length > 0) {
        exportFromConfig()
      } else {
        //without -f, argOpts are mandatory
        if (outputFilePath.length <= 0 || fieldsToExport.length <= 0) {
          parser.showUsage
        } else {
          exportFromQuery()
        }
      }
    }
  }

  def exportFromQuery() {
    val fileWriter = new FileWriter(new File(outputFilePath))

    //header
    fileWriter.write(fieldsToExport.mkString("\t"))
    if (queryAssertions) {
      fileWriter.write("\t")
      fileWriter.write(AssertionCodes.all.map(e => e.name).mkString("\t"))
    }
    fileWriter.write("\n")

    Config.indexDAO.streamIndex(map => {
      counter += 1
      if (counter % 1000 == 0) {
        fileWriter.flush
      }
      val outputLine = fieldsToExport.map(f => {
        if (map.containsKey(f)) map.get(f).toString else ""
      })
      fileWriter.write(outputLine.mkString("\t"))

      if (queryAssertions) {
        //these are multivalue fields
        val assertions = (if (map.containsKey("assertions")) map.get("assertions") else null).asInstanceOf[util.Collection[String]]
        val assertions_passed = (if (map.containsKey("assertions_passed")) map.get("assertions_passed") else null).asInstanceOf[util.Collection[String]]
        val assertions_missing = (if (map.containsKey("assertions_missing")) map.get("assertions_missing") else null).asInstanceOf[util.Collection[String]]

        AssertionCodes.all.map(e => {
          var a = e.name
          if (assertions != null && assertions.contains(a)) {
            fileWriter.write("\tfailed")
          } else if (assertions_passed != null && assertions_passed.contains(a)) {
            fileWriter.write("\tpassed")
          } else if (assertions_missing != null && assertions_missing.contains(a)) {
            fileWriter.write("\tmissing")
          } else {
            fileWriter.write("\t")
          }
        })
      }

      fileWriter.write("\n")
      true
    }, if (queryAssertions) fieldsToExport ++ Array("assertions", "assertions_passed", "assertions_missing") else fieldsToExport,
      query, Array(), orderFields, None)
    Config.indexDAO.shutdown
    fileWriter.flush
    fileWriter.close
  }


  def exportFromConfig() {
    //init
    exportFromConfigInit()

    //run
    exportFromConfigRun()

    //finalize
    exportFromConfigFinish()
  }

  def getHeader(fields: (Array[String], Array[String], Array[Int], Boolean), json: List[Map[String, Object]]): Array[String] = {
    val fields2 = if (fields._2 != null) fields._2 else Array[String]()
    val fields3 = if (fields._3 != null) fields._3.collect { case i: Int => AssertionCodes.getByCode(i).get.name } else Array[String]()
    val fields4 = if (fields._4) Array[String]("user_assertions") else Array[String]()

    (fields._1 ++ fields2 ++ fields3 ++ fields4).collect { case f: String =>
      var h = f
      json.foreach(m => if (m.getOrElse("downloadName", "") == f) h = m.getOrElse("dwcTerm", m.getOrElse("downloadDescription", m.getOrElse("description", m.getOrElse("downloadName", f.toString())))).toString)
      if (h == f) {
        json.foreach(m => if (m.getOrElse("name", "") == f) h = m.getOrElse("dwcTerm", m.getOrElse("downloadDescription", m.getOrElse("description", m.getOrElse("downloadName", f.toString())))).toString)
      }
      h
    }
  }

  def exportFromConfigInit() {
    val jp = new JSONParser()
    filters = jp.parse(FileUtils.readFileToString(new File(cassandraFilterFile))).asInstanceOf[JSONArray]
    for (j <- 0 until numThreads) outputFiles += mutable.ArrayBuffer[CSVWriter]()

    for (i <- 0 until filters.size()) {
      val filter = filters.get(i).asInstanceOf[JSONObject]

      val file = new File(filter.get("output-file").toString)
      var fields = filter.get("list-of-fields").toString.split(",")
      var fieldsBrief = filter.get("list-of-fields-subset1").toString.split(",")
      var fieldsNormal = filter.get("list-of-fields-subset2").toString.split(",")
      val q = filter.get("q").toString
      val fieldValue = filter.get("filter").toString.split(":")

      var layers: Array[String] = if (filter.get("layers").toString.equals("true")) new LayersStore(Config.layersServiceUrl).getFieldIds().asScala.toArray[String] else null
      val qa: Array[Int] = if (filter.get("qa").toString.equals("true")) AssertionCodes.retrieveAll.collect { case e: ErrorCode => e.code }.toArray else null

      //move fields to layers where applicable
      val (actualFields, actualLayers) = fieldsToLayers(fields, layers)

      fieldsList += ((actualFields, actualLayers, qa, filter.get("user-qa").toString.equals("true")))

      if (!file.getParentFile().exists()) {
        file.getParentFile().mkdirs()
      }

      val url = Config.biocacheServiceUrl + "/occurrences/search?facets=institution_uid&facets=collection_uid&facets=data_resource_uid&pageSize=0&flimit=1000&q=" + URLEncoder.encode("q", "UTF-8")
      FileUtils.copyURLToFile(new URL(url), new File(file.getParent() + "/metadata.json"), 20000, 20000)
      new LayersStore(Config.layersServiceUrl).getFieldIds().asScala.toArray
      //create csv writers for all threads
      for (j <- 0 until numThreads) {
        val csv = new CSVWriter(new OutputStreamWriter(new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(file.getPath() + j)))), ',', '"', '\\')
        if (i == 0) outputFiles += mutable.ArrayBuffer[CSVWriter]()
        outputFiles(j) += csv
      }

      filterList += ((fieldValue(0), fieldValue(1)))
    }
  }

  def exportFromConfigFinish() {
    for (list <- outputFiles) {
      for (csv <- list) {
        csv.flush()
        csv.close()
      }
    }

    //join parts
    for (i <- 0 until filters.size()) {
      val filter = filters.get(i).asInstanceOf[JSONObject]
      var fields1 = filter.get("list-of-fields-subset1").toString.split(",")
      var fields2 = filter.get("list-of-fields-subset2").toString.split(",")

      val file = new File(filter.get("output-file").toString.replace(".csv", ".zip"))
      val out = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(file)))
      out.putNextEntry(new ZipEntry(file.getName().replace(".zip", ".csv")))
      val csv = new CSVWriter(new OutputStreamWriter(out))

      logger.info("building: " + file.getPath)

      //header
      var json: List[Map[String, Object]] = JSON.parseFull(Source.fromURL(new URL(Config.biocacheServiceUrl + "/index/fields")).mkString).get.asInstanceOf[List[Map[String, Object]]]
      val fieldsdb = new LayersStore(Config.layersServiceUrl).getFieldIdsAndDisplayNames().asScala
      for (elem <- fieldsdb) {
        val m: List[Map[String, Object]] = List(Map("id" -> elem._1, "name" -> elem._2)); json = json ++ m
      }

      val header = getHeader(fieldsList(i), json)
      csv.writeNext(header)
      csv.flush()

      var columns1: Array[Int] = null
      var values1: Array[String] = null
      var out1: ZipOutputStream = null
      var csv1: CSVWriter = null
      if (fields1.length > 0 && fields1(0).length > 0) {
        logger.info("also building: " + file.getPath().replace(".zip", "-brief.zip"))
        out1 = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(new File(file.getPath().replace(".zip", "-brief.zip")))))
        out1.putNextEntry(new ZipEntry(file.getName().replace(".zip", "-brief.csv")))
        csv1 = new CSVWriter(new OutputStreamWriter(out1))
        val (f, l) = fieldsToLayers(fields1, null)
        val header1 = getHeader((f, l, null, false), json)
        columns1 = header1.collect { case v: String =>
          var i = 0
          for (j <- 0 until header.length) {
            if (header(j) == v) i = j
          }
          i
        }
        csv1.writeNext(header1)
        values1 = new Array[String](header1.length)
      }

      var columns2: Array[Int] = null
      var values2: Array[String] = null
      var out2: ZipOutputStream = null
      var csv2: CSVWriter = null
      if (fields2.length > 0 && fields2(0).length > 0) {
        logger.info("also building: " + file.getPath().replace(".zip", "-standard.zip"))
        out2 = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(new File(file.getPath().replace(".zip", "-standard.zip")))))
        out2.putNextEntry(new ZipEntry(file.getName().replace(".zip", "-standard.csv")))
        csv2 = new CSVWriter(new OutputStreamWriter(out2))
        val (f, l) = fieldsToLayers(fields2, null)
        val header2 = getHeader((f, l, null, false), json)
        columns2 = header2.collect { case v: String =>
          var i = 0
          for (j <- 0 until header.length) {
            if (header(j) == v) i = j
          }
          i
        }
        csv2.writeNext(header2)
        values2 = new Array[String](header2.length)
      }

      //for each thread add contents to csv
      val srcFile = new File(filter.get("output-file").toString)
      for (i <- 0 until numThreads) {
        logger.info("merging: " + srcFile.getPath + i)

        val part = new File(srcFile.getPath + i)

        //open gzip and write to out
        var gz = new GZIPInputStream(new BufferedInputStream(new FileInputStream(part)))
        StreamUtils.copy(gz, out)
        gz.close()

        if (columns1 != null || columns2 != null) {
          gz = new GZIPInputStream(new BufferedInputStream(new FileInputStream(part)))
          val reader = new CSVReader(new InputStreamReader(gz))
          var line: Array[String] = reader.readNext()
          while (line != null) {
            var pos1 = 0
            if (columns1 != null) {
              for (i <- 0 until columns1.length) {
                values1(pos1) = if (line.length > columns1(i)) line(columns1(i)) else ""
                pos1 = pos1 + 1
              }
              csv1.writeNext(values1)
            }

            var pos2 = 0
            if (columns2 != null) {
              for (i <- 0 until columns2.length) {
                values2(pos2) = if (line.length > columns2(i)) line(columns2(i)) else ""
                pos2 = pos2 + 1
              }
              csv2.writeNext(values2)
            }

            line = reader.readNext()
          }
        }
        gz.close()
        part.delete()
      }

      if (csv1 != null) {
        csv1.flush()
        out1.flush()
        out1.closeEntry()
        out1.close()
      }

      if (csv2 != null) {
        csv2.flush()
        out2.flush()
        out2.closeEntry()
        out2.close()
      }

      out.flush()
      out.closeEntry()
      out.close()
    }
  }

  def exportFromConfigRun() {

    //TODO: update this method for cassandra3/solr6

//    val (query, startValue, endValue) = if (dr.isDefined) {
//      ("data_resource_uid:" + dr.get, dr.get + "|", dr.get + "|~")
//    } else {
//      ("*:*", start, end)
//    }
//
//    val ranges = if (keys.isEmpty) {
//      calculateRanges(numThreads, query, startValue, endValue)
//    } else {
//      generateRanges(keys.get, startValue, endValue)
//    }
//
//    var counter = 0
//    val threads = new ArrayBuffer[Thread]
//    ranges.foreach { case (startKey, endKey) =>
//      logger.info("start: " + startKey + ", end key: " + endKey)
//
//      //      val t = new Thread(new BulkColumnExporter(this, counter, startKey, endKey, fieldsList, outputFiles(counter), filterList))
//      //      t.start
//      //      threads += t
//
//      counter += 1
//    }
//
//    //wait for threads to complete and merge all indexes
//    threads.foreach { thread => thread.join }
  }

  def fieldsToLayers(fields: Array[String], layers: Array[String]): (Array[String], Array[String]) = {
    val layerPattern = """^(cl|el)[0-9]+$""".r
    val actualFields: mutable.ArrayBuffer[String] = mutable.ArrayBuffer[String]()
    val actualLayers: mutable.ArrayBuffer[String] = mutable.ArrayBuffer[String]()
    if (layers != null) actualLayers ++= layers
    for (j <- 0 until fields.length) {
      if (layerPattern.findFirstIn(fields(j)).nonEmpty) {
        actualLayers += fields(j)
      } else {
        actualFields += fields(j)
      }
    }

    (actualFields.toArray, actualLayers.toArray)
  }
}
