package au.org.ala.biocache.export

import java.util

import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.util.OptionParser
import java.io.{File, FileWriter}
import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.vocab.{ErrorCodeCategory, AssertionCodes}

/**
 * Utility for exporting a list of fields from the index using SOLR streaming.
 */
object ExportFromIndexStream extends Tool {

  def cmd = "export-stream"
  def desc = "Export from search indexes using streaming"

  var outputFilePath = ""
  var query = "*:*"
  var fieldsToExport = Array[String]()
  var counter = 0
  var orderFields = Array("row_key")
  var queryAssertions = false

  def main(args: Array[String]) {
    val parser = new OptionParser(help) {
      arg("output-file", "The file name for the export file", {
        v: String => outputFilePath = v
      })
      arg("list-of-fields", "CSV list of fields to export", {
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

          AssertionCodes.all.map ( e => {
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
  }
}
