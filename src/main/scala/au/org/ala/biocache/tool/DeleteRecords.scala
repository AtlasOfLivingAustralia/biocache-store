package au.org.ala.biocache.tool

import au.org.ala.biocache.Config
import au.org.ala.biocache.util._
import scala.Some
import au.org.ala.biocache.cmd.Tool

/**
 * Utility to delete records.
 */
object DeleteRecords extends Tool {

  def cmd = "delete-records"
  def desc = "Delete records from the system via query, resource or file of IDs"

  val occurrenceDAO = Config.occurrenceDAO
  val persistenceManager = Config.persistenceManager

  def main(args: Array[String]) {

    var query: Option[String] = None
    var dr: Option[String] = None
    var file: Option[String] = None
    var useUUID = false
    var fieldDelimiter:Char = '\t'
    var hasHeader = false

    val parser = new OptionParser(help) {
      opt("q", "query", "The query to run to obtain the records for deletion e.g. 'year:[2001 TO *]' or 'taxon_name:Macropus'", {
        v: String => query = Some(v)
      })
      opt("dr", "resource", "The data resource to process", {
        v: String => dr = Some(v)
      })
      opt("f", "file", "The file of row keys to delete. Can be an absolute local file path or URL to a file that contains rowkeys or UUIDs.", {
        v: String => file = Some(v)
      })
      opt("hdr", "fileHasHeader", "The supplied file has a header", {
        hasHeader = true
      })
      opt("uuid", "useUUID", "The supplied file contains UUIDs (as opposed to row keys)", {
        useUUID = true
      })
    }

    if (parser.parse(args)) {
      val deletor: Option[RecordDeletor] = {
        if (!query.isEmpty) Some(new QueryDelete(query.get))
        else if (!dr.isEmpty) Some(new QueryDelete("data_resource_uid:" + dr.get))
        else if (file.isDefined) Some(new FileDelete(file.get, useUUID, fieldDelimiter, hasHeader))
        else None
      }
      if (!deletor.isEmpty) {
        deletor.get.deleteFromPersistent
        deletor.get.deleteFromIndex
      } else {
        parser.showUsage
      }
    }
  }
}
