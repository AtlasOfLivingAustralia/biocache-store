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
    val parser = new OptionParser(help) {
      opt("q", "query", "The query to run to obtain the records for deletion e.g. 'year:[2001 TO *]' or 'taxon_name:Macropus'", {
        v: String => query = Some(v)
      })
      opt("dr", "resource", "The data resource to process", {
        v: String => dr = Some(v)
      })
      opt("f", "file", "The file of row keys to delete", {
        v: String => file = Some(v)
      })
    }
    if (parser.parse(args)) {
      val deletor: Option[RecordDeletor] = {
        if (!query.isEmpty) Some(new QueryDelete(query.get))
        else if (!dr.isEmpty) Some(new DataResourceDelete(dr.get))
        else if (file.isDefined) Some(new FileDelete(file.get))
        else None
      }
      println("Starting delete " + query + " " + dr)
      if (!deletor.isEmpty) {
        deletor.get.deleteFromPersistent
        deletor.get.deleteFromIndex
        deletor.get.close
      }
    }
  }
}
