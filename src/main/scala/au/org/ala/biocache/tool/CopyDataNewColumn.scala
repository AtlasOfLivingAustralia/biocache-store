package au.org.ala.biocache.tool

import au.org.ala.biocache.Config
import au.org.ala.biocache.util.OptionParser
import au.com.bytecode.opencsv.CSVReader
import java.io.{File, FileReader}
import au.org.ala.biocache.cmd.Tool

/**
 * Copies the data from one column to another.
 *
 * Optionally deleting the original value
 */
object CopyDataNewColumn extends Tool {

  def cmd = "copy-column"
  def desc = "Copy column utility"

  val occurrenceDAO = Config.occurrenceDAO
  val persistenceManager = Config.persistenceManager

  def main(args: Array[String]): Unit = {

    println("Starting...")

    var sourceColumnFamily = ""
    var targetColumnFamily = ""
    var start: Option[String] = None
    var delete = false
    var dr: Option[String] = None
    var source = ""
    var target = ""
    var rowKeyFile = ""
    var dryRun = true

    val parser = new OptionParser("copy column options") {
      arg("sourceColumnFamily", "The columns family to copy from.", {
        v: String => sourceColumnFamily = v
      })
      arg("sourceColumn", "The column to copy from.", {
        v: String =>  source = v
      })
      arg("targetColumnFamily", "The columns family to copy from.", {
        v: String => targetColumnFamily = v
      })
      arg("targetColumn", "The column to copy from.", {
        v: String => target = v
      })
      opt("s", "start", "The record to start with", {
        v: String => start = Some(v)
      })
      opt("dr", "resource", "The data resource to process", {
        v: String => dr = Some(v)
      })
      opt("delete", "delete the source value", {
        delete = true
      })
      opt("rkf", "rowKeyFile", "Row key file", {
        v: String => rowKeyFile = v
      })
      booleanOpt("dry", "dryRun", "Perform dry run and just output the change that would be made", {
        v: Boolean => dryRun = v
      })
    }

    if (parser.parse(args)) {
      println("Copying from " + source + " to " + target + " in " + sourceColumnFamily)
      val startUuid = if (start.isDefined) start.get else if (dr.isDefined) dr.get + "|" else ""
      val endUuid = if (dr.isDefined) dr.get + "|~" else ""
      val originalStartTime = System.currentTimeMillis
      if(rowKeyFile != ""){

        val reader = new CSVReader(new FileReader(new File(rowKeyFile)))
        var line  = reader.readNext()
        while( line != null){
          val guid = line(0)
          persistenceManager.get(guid, sourceColumnFamily) match {
            case Some(map) =>
              copyData(guid, map, sourceColumnFamily, source, targetColumnFamily, target, delete)
          }
          line  = reader.readNext()
        }
      } else {

        var count = 0
        var startTime = System.currentTimeMillis
        persistenceManager.pageOverSelect(sourceColumnFamily, (guid, map) => {
          copyData(guid, map, sourceColumnFamily, source, targetColumnFamily, target, delete)
          if (count % 1000 == 0) {
            val finishTime = System.currentTimeMillis
            println(count
              + " >> Last key : " + guid
              + ", records per sec: " + 1000f / (((finishTime - startTime).toFloat) / 1000f)
              + ", time taken for " + 1000 + " records: " + (finishTime - startTime).toFloat / 1000f
              + ", total time: " + (finishTime - originalStartTime).toFloat / 60000f + " minutes"
            )
            startTime = System.currentTimeMillis
          }
          count = count + 1
          true
        }, startUuid, endUuid, 1000, "rowKey", "uuid", source)
      }
    }
    //shutdown the persistence
    persistenceManager.shutdown
  }

  def copyData(guid: String,  map: Map[String, String], sourceColumnFamily: String, source: String, targetColumnFamily: String, target: String, delete: Boolean) {
    val sourceValue = map.get(source)
    if (sourceValue.isDefined) {
      persistenceManager.put(guid, targetColumnFamily, target, sourceValue.get)
      if (delete) {
        persistenceManager.deleteColumns(guid, sourceColumnFamily, source)
      }
    }
  }
}