package au.org.ala.biocache.util

import java.io.{File, FileWriter}

import au.com.bytecode.opencsv.CSVWriter
import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool

/**
 * Export the data for the supplied table from a local node.
 */
object ExportLocalNode extends Tool {

  def cmd = "export-local-node"
  def desc = "Export data from local node to a tab delimited file."

  def main(args:Array[String]): Unit = {

    var outputFilePath = ""
    var entity = ""
    var fieldsToExport = Array[String]()
    var counter = 0

    val parser = new OptionParser(help) {
      arg("entity", "The table to export", {
        v: String => entity = v
      })
      arg("output-file", "The path to the file to write to", {
        v: String => outputFilePath = v
      })
      arg("list-of-fields", "The list of fields to export", {
        v: String => fieldsToExport = v.split(",").map(_.trim).toArray
      })
    }

    if (parser.parse(args)) {

      //CSVWriter(Writer writer, char separator, char quotechar, char escapechar, String lineEnd)
      val csvWriter = new CSVWriter(new FileWriter(new File(outputFilePath)), '\t', '|', '$', "\n")

      Config.persistenceManager.pageOverLocal(entity, (rowkey, map, tokenRangeId) => {
        counter += 1
        if (counter % 1000 == 0) {
          println("Exported :" + counter); csvWriter.flush
        }
        val outputLine = fieldsToExport.map { field => getFromMap(map, field) }
        csvWriter.writeNext(outputLine)
        true
      }, 4, Array("rowkey") ++ fieldsToExport )

      csvWriter.flush
      csvWriter.close
    }
  }

   def getFromMap(map: Map[String, String], key: String): String = {
    val value = map.getOrElse(key, "")
    if (value == null) "" else value.toString
  }
}