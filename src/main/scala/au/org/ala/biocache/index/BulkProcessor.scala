package au.org.ala.biocache.index

import org.slf4j.LoggerFactory
import au.org.ala.biocache.util.OptionParser
import au.org.ala.biocache.Config
import scala.collection.mutable.ArrayBuffer
import org.apache.lucene.misc.IndexMergeTool
import au.org.ala.biocache.cmd.Tool

/**
 * A multi-threaded bulk processor that uses the search indexes to create a set a
 * ranges for record IDs. These ranges are then passed to individual threads
 * for processing.
 */
object BulkProcessor extends Tool with Counter with RangeCalculator {

  def cmd = "bulk-processor"
  def desc = "Bulk processor for regenerating indexes and reprocessing the entire cache"

  override val logger = LoggerFactory.getLogger("Indexer-Multithread")

  def main(args: Array[String]) {

    var numThreads = 8
    var pageSize = 200
    var dirPrefix = "/data"
    var keys: Option[Array[String]] = None
    var columns: Option[Array[String]] = None
    var action = ""
    var start, end = ""
    var dr: Option[String] = None
    val validActions = List("range", "process", "index", "col", "repair", "datum")

    val parser = new OptionParser(help) {
      arg("<action>", "The action to perform. Supported values :  range, process or index, col", {
        v: String => action = v
      })
      intOpt("t", "threads", "The number of threads to perform the indexing on. Default is " + numThreads, {
        v: Int => numThreads = v
      })
      intOpt("ps", "pagesize", "The page size for the records. Default is " + pageSize, {
        v: Int => pageSize = v
      })
      opt("p", "prefix", "The prefix to apply to the solr directories. Default is " + dirPrefix, {
        v: String => dirPrefix = v
      })
      opt("k", "keys", "A comma separated list of keys on which to perform the range threads. Prevents the need to query SOLR for the ranges.", {
        v: String => keys = Some(v.split(","))
      })
      opt("s", "start", "The rowKey in which to start the range", {
        v: String => start = v
      })
      opt("e", "end", "The rowKey in which to end the range", {
        v: String => end = v
      })
      opt("dr", "dr", "The data resource over which to obtain the range", {
        v: String => dr = Some(v)
      })
      opt("c", "columns", "The columns to export", {
        v: String => columns = Some(v.split(","))
      })
    }

    if (parser.parse(args)) {
      if (validActions.contains(action)) {
        val (query, start, end) = if (dr.isDefined){
          ("data_resource_uid:" + dr.get, dr.get + "|", dr.get + "|~")
        } else {
          ("*:*", "", "")
        }

        Config.persistenceManager.get("test", "occ", "blah")

        val ranges = if (keys.isEmpty){
          calculateRanges(numThreads, query, start, end)
        } else {
          generateRanges(keys.get, start, end)
        }

        if (action == "range") {
          logger.info(ranges.mkString("\n"))
        } else if (action != "range") {
          var counter = 0
          val threads = new ArrayBuffer[Thread]
          val columnRunners = new ArrayBuffer[ColumnReporterRunner]
          val solrDirs = new ArrayBuffer[String]
          solrDirs += (dirPrefix + "/solr/bio-proto-merged/data/index")
          ranges.foreach(r => {
            logger.info("start: " + r._1 + ", end key: " + r._2)

            val ir = {
              if (action == "datum") {
                new DatumRecordsRunner(this, counter, r._1, r._2)
              } else if (action == "repair") {
                new RepairRecordsRunner(this, counter, r._1, r._2)
              } else if (action == "index") {
                solrDirs += (dirPrefix + "/solr-create/bio-proto-thread-" + counter + "/data/index")
                new IndexRunner(this,
                  counter,
                  r._1,
                  r._2,
                  dirPrefix + "/solr-template/bio-proto/conf",
                  dirPrefix + "/solr-create/bio-proto-thread-" + counter + "/conf",
                  pageSize
                )
              } else if (action == "process") {
                new ProcessRecordsRunner(this, counter, r._1, r._2)
              } else if (action == "col") {
                if (columns.isEmpty) {
                  new ColumnReporterRunner(this, counter, r._1, r._2)
                } else {
                  new ColumnExporter(this, counter, r._1, r._2, columns.get.toList)
                }
              } else {
                new Thread()
              }
            }
            val t = new Thread(ir)
            t.start
            threads += t
            if (ir.isInstanceOf[ColumnReporterRunner]) {
              columnRunners += ir.asInstanceOf[ColumnReporterRunner]
            }
            counter += 1
          })

          //wait for threads to complete and merge all indexes
          threads.foreach(thread => thread.join)

          if (action == "index") {
            //TODO - might be worth avoiding optimisation
            IndexMergeTool.main(solrDirs.toArray)
            Config.persistenceManager.shutdown
            logger.info("Waiting to see if shutdown")
            System.exit(0)
          } else if (action == "col") {
            var allSet: Set[String] = Set()
            columnRunners.foreach(c => allSet ++= c.myset)
            allSet = allSet.filterNot(it => it.endsWith(".p") || it.endsWith(".qa"))
            logger.info("All set: " + allSet)
          }
        }
      }
    }
  }
}
