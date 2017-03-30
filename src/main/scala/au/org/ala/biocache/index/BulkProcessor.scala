package au.org.ala.biocache.index

import java.io._

import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.index.lucene.LuceneIndexing
import au.org.ala.biocache.tool.ProcessRecords
import au.org.ala.biocache.util.OptionParser
import org.apache.commons.io.FileUtils
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{ConcurrentMergeScheduler, IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.Version
import org.apache.solr.core.{SolrConfig, SolrResourceLoader}
import org.apache.solr.schema.IndexSchemaFactory
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
    var threadsPerWriter = 1
    var threadsPerProcess = 1
    var ramPerWriter = 50
    var writerSegmentSize = 500000
    var writerBufferSize = 3000
    var processorBufferSize = 100
    var singleWriter = false
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
    var cassandraFilterFile = ""

    val parser = new OptionParser(help) {
      arg("<action>", "The action to perform. Supported values :  range, process or index, col", {
        v: String => action = v
      })
      intOpt("t", "threads", "The number of threads to perform the indexing on. Default is " + numThreads, {
        v: Int => numThreads = v
      })
      intOpt("wt", "writerthreads", "The number of threads for each indexing writer. There is 1 writer for each -t. Default is " + threadsPerWriter, {
        v: Int => threadsPerWriter = v
      })
      opt("sw", "singlewriter", "Use one index writer instead of one writer per 'thread'. Default is " + singleWriter, { singleWriter = true })
      intOpt("wb", "writerbuffer", "Size of indexing write buffer. The default is " + writerBufferSize, {
        v: Int => writerBufferSize = v
      })
      intOpt("pt", "processthreads", "The number of threads for each indexing process. There is 1 process for each -t. Default is " + threadsPerProcess, {
        v: Int => threadsPerProcess = v
      })
      intOpt("pb", "processbuffer", "Size of the indexing process buffer. Default is " + processorBufferSize, {
        v: Int => processorBufferSize = v
      })
      intOpt("r", "writerram", "Ram allocation for each writer (MB). There is 1 writer for each -t. Default is " + ramPerWriter, {
        v: Int => ramPerWriter = v
      })
      intOpt("ws", "writersegmentsize", "Maximum number of occurrences in a writer segment. There is 1 writer for each -t. Default is " + writerSegmentSize, {
        v: Int => writerSegmentSize = v
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

        //init for luceneIndexing
        var luceneIndexing: ArrayBuffer[LuceneIndexing] = new ArrayBuffer[LuceneIndexing]
        if (action == "index") {
          val h = dirPrefix + "/solr-create/biocache-thread-0/conf"
          //solr-create/thread-0/conf
          val newIndexDir = new File(h)
          if (newIndexDir.exists) {
            FileUtils.deleteDirectory(newIndexDir.getParentFile())
          }
          FileUtils.forceMkdir(newIndexDir)

          //CREATE a copy of SOLR home
          val sourceConfDir = new File(dirPrefix + "/solr-template/biocache/conf") //solr-template/biocache/conf
          FileUtils.copyDirectory(sourceConfDir, newIndexDir)

          //COPY solr-template/biocache/solr.xml  -> solr-create/biocache-thread-0/solr.xml
          FileUtils.copyFileToDirectory(new File(sourceConfDir.getParent + "/solr.xml"), newIndexDir.getParentFile)

          val schema = IndexSchemaFactory.buildIndexSchema("schema.xml",
            SolrConfig.readFromResourceLoader(new SolrResourceLoader(dirPrefix + "/solr-template/biocache"), "solrconfig.xml"))

          if (singleWriter) {
            luceneIndexing += new LuceneIndexing(schema, writerSegmentSize.toLong, newIndexDir.getParent() + "/data0-",
              ramPerWriter, writerBufferSize, writerBufferSize / (2), threadsPerWriter)
          } else {
            for (i <- 0 until numThreads) {
              luceneIndexing += new LuceneIndexing(schema, writerSegmentSize.toLong, newIndexDir.getParent() + "/data" + i + "-",
                ramPerWriter, writerBufferSize, writerBufferSize / (threadsPerWriter + 2), threadsPerWriter)
            }
          }
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

            val ir : Runnable = {
              if (action == "datum") {
                new DatumRecordsRunner(this, counter, startKey, endKey)
              } else if (action == "repair") {
                new RepairRecordsRunner(this, counter, startKey, endKey)
              } else if (action == "index") {
                //solrDirs += (dirPrefix + "/solr-create/biocache-thread-" + counter + "/data/index")
                val index = if (!singleWriter) luceneIndexing(counter % numThreads) else luceneIndexing(0)
                new IndexRunner(this,
                  counter,
                  startKey,
                  endKey,
                  dirPrefix + "/solr-template/biocache/conf",
                  dirPrefix + "/solr-create/biocache-thread-" + counter + "/conf",
                  pageSize,
                  index,
                  threadsPerProcess,
                  processorBufferSize,
                  singleWriter
                )
              } else if (action == "load-sampling") {
                new LoadSamplingRunner(this, counter, startKey, endKey)
//              } else if (action == "avro-export") {
//                new AvroExportRunner(this, counter, startKey, endKey)
              } else if (action == "col" || action == "column-export") {
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
            val dirs = new ArrayBuffer[String]()

            if (singleWriter) {
              luceneIndexing(0).close(true, false)
            }

            luceneIndexing.foreach { i =>
              for (j <- 0 until i.getOutputDirectories.size()) {
               dirs += i.getOutputDirectories.get(j).getPath()
              }
            }

            luceneIndexing = null
            threads.clear()
            System.gc()

            val mem = Math.max((Runtime.getRuntime().freeMemory() * 0.75) / 1024 / 1024, numThreads * ramPerWriter).toInt

            logger.info("Merging index segments")
            IndexMergeTool.merge(dirPrefix + "/solr/merged", dirs.toArray, forceMerge, mergeSegments, deleteSources, mem)
            //LuceneIndexing.merge(dirs, new File(dirPrefix + "/solr/merged"), mem, 8, forceMerge)
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

    val writerConfig = (new IndexWriterConfig(Version.LATEST, null))
      .setOpenMode(OpenMode.CREATE)
      .setRAMBufferSizeMB(rambuffer)

    writerConfig.setMergeScheduler(new ConcurrentMergeScheduler)
    writerConfig.getMergeScheduler.asInstanceOf[ConcurrentMergeScheduler].setMaxMergesAndThreads(Math.min(4, mergeSegments), mergeSegments)
    writerConfig.getMergeScheduler.asInstanceOf[ConcurrentMergeScheduler].setMergeThreadPriority(3)

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
