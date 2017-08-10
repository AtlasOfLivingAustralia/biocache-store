package au.org.ala.biocache.tool

import au.org.ala.biocache._
import java.io.{BufferedOutputStream, FileOutputStream}
import java.io.File
import au.org.ala.biocache.index.{IndexRecords, IndexDAO}
import au.org.ala.biocache.util.OptionParser
import au.org.ala.biocache.cmd.Tool

/**
 * Reprocesses and reindexes a select set of records.  The records will
 * be obtained through a query to the index.
 */
object ReprocessIndexSelect extends Tool {

  def cmd = "index-query"
  def desc = "Reindex the records that match a query"

  def main(args: Array[String]): Unit = {
    var query: Option[String] = None;
    var threads = 4
    var exist = false;
    var indexOnly = false
    var startUuid: Option[String] = None
    val parser = new OptionParser(help) {
      opt("q", "query", "The query to run e.g. 'year:[2001 TO *]' or 'taxon_name:Macropus'", {
        v: String => query = Some(v)
      })
      intOpt("t", "thread", "The number of threads to use", { v: Int => threads = v })
      opt("exist", "use the existing list of row keys", { exist = true })
      opt("s", "start", "The record to start processing with", { v: String => startUuid = Some(v) })
      opt("index", "reindex only - do not reprocess", { indexOnly = true })
    }

    if (parser.parse(args)) {
      if (!query.isEmpty) {
        reprocessIndex(query.get, threads, exist, startUuid, indexOnly)
      } else {
        parser.showUsage
      }
    }
  }

  def reprocessIndex(query: String, threads: Int, exist: Boolean, startUuid: Option[String], indexOnly: Boolean) {
    //get the list of rowKeys to be processed.
    val indexer = Config.getInstance(classOf[IndexDAO]).asInstanceOf[IndexDAO]
    val file = new File(Config.tmpWorkDir + "/reprocess_index_rowkeys.out")
    if (!exist) {
      val out = new BufferedOutputStream(new FileOutputStream(file))
      indexer.writeRowKeysToStream(query, out)
      out.flush
      out.close
    }
    if (!indexOnly) {
      ProcessRecords.processFileOfRowKeys(file, threads)
    }
    IndexRecords.indexList(file)
  }
}