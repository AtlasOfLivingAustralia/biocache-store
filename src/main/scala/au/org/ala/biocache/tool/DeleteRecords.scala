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
  def desc = "Delete records from the system via query to SOLR, resource or file of IDs. "

  val occurrenceDAO = Config.occurrenceDAO
  val persistenceManager = Config.persistenceManager

  def main(args: Array[String]) {

    var query: Option[String] = None
    var dr: Option[String] = None
    var lastLoadedDate: Option[String] = None
    var file: Option[String] = None
    var fieldDelimiter:Char = '\t'

    var hasHeader = false

    // https://records-ws.nbnatlas.org/occurrences/search?q=last_load_date:[2017-10-01T00:00:00Z TO *]

    val parser = new OptionParser(help) {
      opt("q", "query", "The query to run to obtain the records for deletion e.g. 'year:[2001 TO *]' or 'taxon_name:Macropus'", {
        v: String => query = Some(v)
      })
      opt("dr", "resource", "The data resource to process", {
        v: String => dr = Some(v)
      })
      opt("lld", "last-loaded", "Delete records from a data resource that where not loade before a date (yyy-MM-dd format)", {
        v: String => lastLoadedDate = Some(v)
      })
      opt("f", "file", "The file of row keys to delete. Can be an absolute local file path or URL to a file that contains rowkeys or UUIDs.", {
        v: String => file = Some(v)
      })
      opt("hdr", "fileHasHeader", "The supplied file has a header", {
        hasHeader = true
      })
    }

    if (parser.parse(args)) {
      val deletor: Option[RecordDeletor] = {
        if (!query.isEmpty) Some(new QueryDelete(query.get))
        else if (!dr.isEmpty && !lastLoadedDate.isEmpty) Some(new QueryDelete("data_resource_uid:" + dr.get + s" AND -last_load_date:[${lastLoadedDate.get}T00:00:00Z TO *]"))
        else if (!dr.isEmpty ) Some(new QueryDelete("data_resource_uid:" + dr.get))
        else if (file.isDefined) Some(new FileDelete(file.get, hasHeader))
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
