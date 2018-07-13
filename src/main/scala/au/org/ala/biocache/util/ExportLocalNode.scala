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

  val elR = "el[0-9]{1,}".r
  val clR = "cl[0-9]{1,}".r

  def main(args:Array[String]): Unit = {

    var outputFilePath = ""
    var entity = ""
    var fieldsToExport = Array[String]()
    var counter = 0
    var threads = 4
    var separatorChar = '\t'
    var quoteChar = '|'
    var escapeChar = '$'
    var lineEnd = "\n"

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
      intOpt("t", "no-of-threads", "The number of threads to use", { v: Int => threads = v })

      opt("sep", "separator-char", "Field separator. Default is tab.", { v: String => separatorChar = v.trim.charAt(0) })
      opt("quote", "quote-char", "Quote character for enclosing fields. Default is " + quoteChar, { v: String => quoteChar = v.trim.charAt(0) })
      opt("esc", "escape-char", "Escape character. Default is " + escapeChar, { v: String => escapeChar = v.trim.charAt(0) })
      opt("le", "line-end", "The number of threads to use", { v: String => lineEnd = v.trim })
    }

    val elR = "el[0-9]{1,}".r
    val clR = "cl[0-9]{1,}".r

    if (parser.parse(args)) {

      //CSVWriter(Writer writer, char separator, char quotechar, char escapechar, String lineEnd)
      val csvWriter = new CSVWriter(new FileWriter(new File(outputFilePath)), separatorChar, quoteChar, escapeChar, lineEnd)

      //if el889 and cl338 requested, then "cl" and "el" and parse JSON
      val elFieldRequest = !fieldsToExport.find { !elR.findFirstIn(_).isEmpty }.isEmpty
      val clFieldRequest = !fieldsToExport.find { !clR.findFirstIn(_).isEmpty }.isEmpty

      var fieldList = Array("rowkey") ++ fieldsToExport
      if (elFieldRequest){
        //remove el123
        fieldList = fieldList.filter { elR.findFirstIn( _).isEmpty }
        fieldList =  fieldList :+ "el_p"
      }
      if (clFieldRequest){
        fieldList = fieldList.filter { clR.findFirstIn( _).isEmpty }
        fieldList =  fieldList :+ "cl_p"
      }

      Config.persistenceManager.pageOverLocal(entity, (rowkey, map, tokenRangeId) => {
        counter += 1
        if (counter % 1000 == 0) {
          println("Exported : " + counter); csvWriter.flush
        }

        //parse JSON field
        val el:Option[collection.Map[String, Object]] = if (elFieldRequest){
          Some(Json.toMap(map.getOrElse("el_p", "{}")))
        } else {
          None
        }

        //parse JSON field
        val cl:Option[collection.Map[String,String]] = if (clFieldRequest){
          Some(Json.toStringMap(map.getOrElse("cl_p", "{}")))
        } else {
          None
        }

        val outputLine = fieldsToExport.map { field => getFromMap(map, el, cl, field) }
        csvWriter.writeNext(outputLine)
        true
      }, threads, fieldList)

      csvWriter.flush
      csvWriter.close
    }
  }

  def getFromMap(map: Map[String, String], el: Option[collection.Map[String, Object]], cl: Option[collection.Map[String, String]],  key: String): String = {
    //handle cl and el fields if requested
    if(!el.isEmpty && !elR.findFirstIn(key).isEmpty && el.get != null){
      val value = el.get.getOrElse(key, null)
      if (value == null)
        ""
      else
        value.toString
    } else if(!cl.isEmpty && !clR.findFirstIn(key).isEmpty && cl.get != null){
      cl.get.getOrElse(key, "").toString
    } else {
      val value = map.getOrElse(key, "")
      if (value == null)
        ""
      else
        value.toString
    }
  }
}