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

  def desc = "Export from search indexes using SOLR streaming."

  override val logger = LoggerFactory.getLogger("ExportFromIndexStream")

  var numThreads = 8
  var outputFilePath = ""
  var query = "*:*"
  var fieldsToExport = Array[String]()
  var orderFields = Array("row_key")
  var queryAssertions = false

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
    }

    if (parser.parse(args)) {
      if (outputFilePath.length <= 0 || fieldsToExport.length <= 0) {
        parser.showUsage
      } else {
        exportFromQuery()
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
        logger.info("Exported records: $counter")
        fileWriter.flush
      }
      val outputLine = fieldsToExport.map { f =>
        if (map.containsKey(f)) map.get(f).toString else ""
      }
      fileWriter.write(outputLine.mkString("\t"))

//      if (queryAssertions) {
//        //these are multivalue fields
//        val assertions = (if (map.containsKey("assertions")) map.get("assertions") else null).asInstanceOf[util.Collection[String]]
//        val assertions_passed = (if (map.containsKey("assertions_passed")) map.get("assertions_passed") else null).asInstanceOf[util.Collection[String]]
//        val assertions_missing = (if (map.containsKey("assertions_missing")) map.get("assertions_missing") else null).asInstanceOf[util.Collection[String]]
//
//        AssertionCodes.all.map(e => {
//          var a = e.name
//          if (assertions != null && assertions.contains(a)) {
//            fileWriter.write("\tfailed")
//          } else if (assertions_passed != null && assertions_passed.contains(a)) {
//            fileWriter.write("\tpassed")
//          } else if (assertions_missing != null && assertions_missing.contains(a)) {
//            fileWriter.write("\tmissing")
//          } else {
//            fileWriter.write("\t")
//          }
//        })
//      }

      fileWriter.write("\n")
      true
    }, if (queryAssertions) fieldsToExport ++ Array("assertions", "assertions_passed", "assertions_missing") else fieldsToExport,
      query, Array(), orderFields, None)
    Config.indexDAO.shutdown
    fileWriter.flush
    fileWriter.close
  }

  def getHeader(fields: (Array[String], Array[String], Array[Int], Boolean), json: List[Map[String, Object]]): Array[String] = {

    val fields2 = if (fields._2 != null) fields._2 else Array[String]()
    val fields3 = if (fields._3 != null) fields._3.collect { case i: Int => AssertionCodes.getByCode(i).get.name } else Array[String]()
    val fields4 = if (fields._4) Array[String]("user_assertions") else Array[String]()

    (fields._1 ++ fields2 ++ fields3 ++ fields4).collect { case f: String =>
      var h = f
      json.foreach(m => if (m.getOrElse("downloadName", "") == f) {
        h = m.getOrElse(
          "dwcTerm",
          m.getOrElse("downloadDescription",
            m.getOrElse("description",
             m.getOrElse("downloadName", f.toString()))
          )
        ).toString
      })
      if (h == f) {
        json.foreach(m => if (m.getOrElse("name", "") == f) {
          h = m.getOrElse(
            "dwcTerm",
            m.getOrElse("downloadDescription",
              m.getOrElse("description",
                m.getOrElse("downloadName", f.toString()))
            )
          ).toString
        })
      }
      h
    }
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
