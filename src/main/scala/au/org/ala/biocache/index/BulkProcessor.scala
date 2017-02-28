package au.org.ala.biocache.index

import java.io.File

import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.tool.ProcessRecords
import au.org.ala.biocache.util.OptionParser
import org.apache.commons.io.FileUtils
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.Version
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * A multi-threaded bulk processor that uses the search indexes to create a set a
 * ranges for record IDs. These ranges are then passed to individual threads
 * for processing.
 *
 * This tools is used for:
 *
 * 1) Reprocessing the entire dataset.
 * 2) Resampling the entire dataset.
 * 3) Creating a brand new complete index offline.
 */
object BulkProcessor extends Tool with Counter with RangeCalculator {

  def cmd = "bulk-processor"
  def desc = "Bulk processor for regenerating indexes and reprocessing the entire cache"

  override val logger = LoggerFactory.getLogger("BulkProcessor")

  def main(args: Array[String]) {

    var numThreads = 8
    var pageSize = 200
    var dirPrefix = "/data/biocache-reindex"
    var keys: Option[Array[String]] = None
    var columns: Option[Array[String]] = None
    var action = ""
    var start, end = ""
    var dr: Option[String] = None
    val validActions = List("range", "process", "index", "col", "repair", "datum", "load-sampling", "avro-export")
    var forceMerge = true
    var mergeSegments = 1
    var deleteSources = false
    var includeRowkey = true
    var charSeparator = '\t'

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
      opt("c", "columns", "The columns to export", {
        v: String => columns = Some(v.split(","))
      })
      opt("erk", "exclude-rowkey", "Exclude rowkey from export.", {
        includeRowkey = false
      })
      opt("sep", "char=separator", "Character separator for CSV exports. Defaults to tab.", {
        v: String => charSeparator = v.trim().charAt(0)
      })
      opt("sm", "skipMerge", "Force merge of segments. Default is " + forceMerge + ". For index only.", { forceMerge = false })
      intOpt("ms", "max segments", "Max merge segments. Default " + mergeSegments + ". For index only.", {
        v: Int => mergeSegments = v
      })
      opt("ds", "delete-sources", "Delete sources if successful. Defaults to " + deleteSources + ". For index only.", {
        deleteSources = true
      })
    }

    if (parser.parse(args)) {
      if (validActions.contains(action)) {
        val (query, startValue, endValue) = if (dr.isDefined){
          ("data_resource_uid:" + dr.get, dr.get + "|", dr.get + "|~")
        } else {
          ("*:*", start, end)
        }

        val ranges = if (keys.isEmpty){
          calculateRanges(numThreads, query, startValue, endValue)
        } else {
          generateRanges(keys.get, startValue, endValue)
        }

        if (action == "range") {
          logger.info(ranges.mkString("\n"))
        } else if (action == "process") {
          ProcessRecords.processRecords(numThreads, Option(start), dr, false, null, Option(end))
        } else {
          var counter = 0
          val threads = new ArrayBuffer[Thread]
          val columnRunners = new ArrayBuffer[ColumnReporterRunner]
          val solrDirs = new ArrayBuffer[String]
          ranges.foreach { case (startKey, endKey)  =>
            logger.info("start: " + startKey + ", end key: " + endKey)

            val ir = {
              if (action == "datum") {
                new DatumRecordsRunner(this, counter, startKey, endKey)
              } else if (action == "repair") {
                new RepairRecordsRunner(this, counter, startKey, endKey)
              } else if (action == "index") {
                solrDirs += (dirPrefix + "/solr-create/biocache-thread-" + counter + "/data/index")
                new IndexRunner(this,
                  counter,
                  startKey,
                  endKey,
                  dirPrefix + "/solr-template/biocache/conf",
                  dirPrefix + "/solr-create/biocache-thread-" + counter + "/conf",
                  pageSize
                )
              } else if (action == "load-sampling") {
                new LoadSamplingRunner(this, counter, startKey, endKey)
              } else if (action == "avro-export") {
//                new AvroExportRunner(this, counter, startKey, endKey)
//              } else if (action == "col" || action == "column-export") {
                if (columns.isEmpty) {
                  new ColumnReporterRunner(this, counter, startKey, endKey)
                } else {
                  new ColumnExporter(this, counter, startKey, endKey, columns.get.toList, includeRowkey, charSeparator)
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
          }

          //wait for threads to complete and merge all indexes
          threads.foreach { thread => thread.join }

          if (action == "index") {
            logger.info("Merging index segments")
            IndexMergeTool.merge(dirPrefix + "/solr/merged", solrDirs.toArray, forceMerge, mergeSegments, deleteSources)
            logger.info("Shutting down persistence manager")
            Config.persistenceManager.shutdown
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

/**
 * Thin wrapper around SOLR index merge tool that allows it to be incorporated
 * into the CMD2.
 *
 * TODO add support for directory patterns e.g. /data/solr-create/biocache-thread-{wildcard}/data/index
 */
object IndexMergeTool extends Tool {

  def cmd = "index-merge"
  def desc = "Merge indexes "
  val logger = LoggerFactory.getLogger("IndexMergeTool")

  def main(args:Array[String]){

    var mergeDir = ""
    var directoriesToMerge = Array[String]()
    var forceMerge = true
    var mergeSegments = 1
    var deleteSources = false
    var ramBuffer = 4096.0d

    val parser = new OptionParser(help) {
      arg("<merge-dr>", "The output path for the merged index", {
        v: String => mergeDir = v
      })
      arg("<to-merge>", "Pipe separated list of directories to merge", {
        v: String => directoriesToMerge = v.split('|').map(x => x.trim)
      })
      opt("sk", "skipMerge", "Skip merge of segments.", {
        forceMerge = false
      })
      intOpt("ms", "max-segments", "Max merge segments. Default " + mergeSegments, {
        v: Int => mergeSegments = v
      })
      doubleOpt("ram", "ram-buffer", "RAM buffer size. Default " + ramBuffer, {
        v: Double => ramBuffer = v
      })
      opt("ds", "delete-sources", "Delete sources if successful. Defaults to " + deleteSources, {
        deleteSources = true
      })
    }
    if (parser.parse(args)) {
      merge(mergeDir, directoriesToMerge, forceMerge, mergeSegments, deleteSources, ramBuffer)
    }
  }

  /**
   * Merge method that wraps SOLR merge API
 *
   * @param mergeDir
   * @param directoriesToMerge
   * @param forceMerge
   * @param mergeSegments
   */
  def merge(mergeDir: String, directoriesToMerge: Array[String], forceMerge: Boolean, mergeSegments: Int, deleteSources:Boolean, rambuffer:Double = 4096.0d) {
    val start = System.currentTimeMillis()

    logger.info("Merging to directory:  " + mergeDir)
    directoriesToMerge.foreach(x => println("Directory included in merge: " + x))

    val mergeDirFile = new File(mergeDir)
    if (mergeDirFile.exists()) {
      //clean out the directory
      mergeDirFile.listFiles().foreach(f => FileUtils.forceDelete(f))
    } else {
      FileUtils.forceMkdir(mergeDirFile)
    }

    val mergedIndex = FSDirectory.open(mergeDirFile)

    val writerConfig = (new IndexWriterConfig(Version.LATEST, new StandardAnalyzer()))
      .setOpenMode(OpenMode.CREATE)
      .setRAMBufferSizeMB(rambuffer)

    val writer = new IndexWriter(mergedIndex, writerConfig)
    val indexes = directoriesToMerge.map(dir => FSDirectory.open(new File(dir)))

    logger.info("Adding indexes...")
    writer.addIndexes(indexes:_*)

    if (forceMerge) {
      logger.info("Full merge...")
      writer.forceMerge(mergeSegments)
    } else {
      logger.info("Skipping merge...")
    }

    writer.close()
    val finish = System.currentTimeMillis()
    logger.info("Merge complete:  " + mergeDir + ". Time taken: " +((finish-start)/1000)/60 + " minutes")

    if(deleteSources){
      logger.info("Deleting source directories")
      directoriesToMerge.foreach(dir => FileUtils.forceDelete(new File(dir)))
      logger.info("Deleted source directories")
    }
  }
}
