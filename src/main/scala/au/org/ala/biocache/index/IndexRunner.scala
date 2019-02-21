package au.org.ala.biocache.index

import java.io.File
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong

import au.org.ala.biocache._
import au.org.ala.biocache.index.lucene.LuceneIndexing
import au.org.ala.biocache.persistence.DataRow
import au.org.ala.biocache.util.JMX
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * A runnable thread for creating a complete new index.
  *
  * tuning:
  *
  * queues and controlling config:
  * for each thread(--thread) there is a stack of queues
  * Cassandra(--pagesize) ->
  * Processing(solr.batch.size) ->
  * LuceneDocuments(solr.hard.commit.size) ->
  * CommitBatch(solr.hard.commit.size / (writerthreads + 1))
  *
  * --processorBufferSize can be small, (100). Ideally it is the same as --pagesize, memory permitting.
  *
  * --writterBufferSize should be large (10000) to reduce commit batch overhead.
  *
  * --writerram (default=200 MB) is large to reduce the number of disk flushes. The impact can be observed
  * comparing 'index docs committed/in ram' values. Note that memory required is --writerram * --threads MB.
  * The lesser size of --writeram to exceed is that it is large enough to
  * fit --writterBufferSize / (writerthreads + 2) documents. The average doc size
  * can be determined from 'in ram/ram MB'. The larger --writerram is, the less merging that will be required
  * when finished.
  *
  * --pagesize should be adjusted for cassandra read performance (1000)
  *
  * --threads can be reduced but this may have the largest impact on performance. Compare 'average records/s'
  * for different settings.
  *
  * --threads increase should more quickly add documents to the Processing queue, if it is consistently low.
  * This has a large impact on memory required.
  *
  * --processthreads should be increased if the Processing queue is consistently full. Also depends on available CPU.
  *
  * --writerthreads may be increased if the LuceneDocuments queue is consistently full. This has low impact on
  * writer performance. Also depends on available CPU.
  *
  * --writersegmentsize should not be low because more segments are produced and will need to be merged at the end of
  *indexing. If it is too large the performance on producing the lucene index diminish over time, for each --thread.
  *
  * To run indexing without the queues Processing, LuceneDocuments, CommitBatch and their associated threads,
  * use --processthreads=0 and --writerthreads=0. This is for low mem/slow disk/low number of CPU systems.
  *
  * After adjusting the number of threads, the bottleneck; cassandra, processing or lucene, can be observed with
  * cassandraTime, processingTime, and solrTime or the corresponding queue sizes.
  *
  * @param centralCounter
  * @param confDirPath
  * @param pageSize
  * @param luceneIndexing
  * @param processingThreads
  * @param processorBufferSize
  * @param singleWriter
  */
class IndexRunner(centralCounter: Counter,
                  confDirPath: String,
                  pageSize: Int = 200,
                  luceneIndexing: Seq[LuceneIndexing],
                  processingThreads: Integer = 1,
                  processorBufferSize: Integer = 100,
                  singleWriter: Boolean = false,
                  test: Boolean = false,
                  numThreads: Int = 2,
                  maxRecordsToIndex:Int = -1
                 ) extends Runnable {

  val logger: Logger = LoggerFactory.getLogger("IndexRunner")

  var startTimeFinal: AtomicLong = new AtomicLong() // do not set startTimeFinal until after the first processed record

  val directoryList = new java.util.ArrayList[File]

  val timing = new AtomicLong(0)

  val threadId = 0

  def run() {

    //need to synchronize luceneIndexing for docBuilder.index() when indexer.commitThreadCount == 0
    val lock: Array[Object] = new Array[Object](luceneIndexing.size)
    for (i <- luceneIndexing.indices) {
      lock(i) = new Object()
    }

    //solr-create/thread-0/conf
    val newIndexDir = new File(confDirPath)

    val indexer = new SolrIndexDAO(newIndexDir.getParentFile.getParent, Config.excludeSensitiveValuesFor, Config.extraMiscFields)

    val counter = new AtomicLong(0)
    val start = System.currentTimeMillis
    val startTime = new AtomicLong(System.currentTimeMillis)
    val finishTime = new AtomicLong(0)

    val csvFileWriter = if (Config.exportIndexAsCsvPath.length > 0) {
      indexer.getCsvWriter()
    } else {
      null
    }

    val csvFileWriterSensitive = if (Config.exportIndexAsCsvPath.length > 0) {
      indexer.getCsvWriter(true)
    } else {
      null
    }

    val queue: LinkedBlockingQueue[(String, DataRow)] = new LinkedBlockingQueue[(String, DataRow)](processorBufferSize)

    val threads = mutable.ArrayBuffer[ProcessThread]()

    val timeCounter = new AtomicLong(0L)

    if (processingThreads > 0 && luceneIndexing != null) {
      for (i <- 0 until processingThreads) {
        var t = new ProcessThread()

        t.timeCounterL = timeCounter

        //evenly distribute processors between LuceneIndexing objects
        val idx = i % luceneIndexing.size
        t.luceneIndexer = luceneIndexing(idx)

        //need to synchronize luceneIndexing for docBuilder.index() when indexer.commitThreadCount == 0
        t.lock = lock(idx)

        t.start()
        threads += t
      }
    }

    class ProcessThread extends Thread() {
      var recordsProcessed = 0
      var luceneIndexer: LuceneIndexing = null
      var lock: Object = null
      var timeCounterL: AtomicLong = null

      override def run() {

        // Specify the SOLR config to use
        indexer.solrConfigPath = newIndexDir.getAbsolutePath + "/solrconfig.xml"
        var continue = true
        while (continue) {
          val (guid, row) = queue.take()
          if (guid == null) {
            continue = false
          } else {
            try {
              val t1 = System.nanoTime()

              val t2 = indexer.indexFromArray(
                guid,
                row,
                docBuilder = luceneIndexer.getDocBuilder,
                lock = lock,
                test = test)

              if (timeCounterL.get() == 0) {
                timeCounterL.set(System.currentTimeMillis())
                startTimeFinal.set(System.currentTimeMillis())
              }

              timing.addAndGet(System.nanoTime() - t1 - t2)
            } catch {
              case e: InterruptedException => throw e
              case e: Exception => logger.error("guid:" + guid + ", " + e.getMessage, e)
            }
          }
          recordsProcessed += 1
        }
      }
    }

    val t2Total = new AtomicLong(0L)
    var t2 = System.nanoTime()
    var uuidIdx = -1

    //page through and create and index for this range
    Config.persistenceManager.pageOverSelectArray("occ", (guid, row) => {

      t2Total.addAndGet(System.nanoTime() - t2)

      val currentCounter = counter.incrementAndGet().toInt

      //ignore the record if it has the guid that is the startKey this is because it will be indexed last by the previous thread.
      try {
        if (uuidIdx == -1) {
          uuidIdx = row.getIndexOf("rowkey")
        }
        if (!StringUtils.isEmpty(row.getString(uuidIdx))) {
          val t1 = System.nanoTime()
          var t2 = 0L
          if (processingThreads > 0) {
            queue.put((row.getString(uuidIdx), row))
          } else {
            t2 = indexer.indexFromArray(row.getString(uuidIdx), row)
            timing.addAndGet(System.nanoTime() - t1 - t2)
          }
        }
      } catch {
        case e: Exception =>
          logger.error("Problem indexing record: " + guid + " " + e.getMessage, e)
          if (logger.isDebugEnabled) {
            logger.debug("Error during indexing: " + e.getMessage, e)
          }
      }

      if (currentCounter % 1000 == 0) {

        centralCounter.setCounter(currentCounter.toInt)
        finishTime.set(System.currentTimeMillis)
        if(currentCounter.toInt > 0) {
          centralCounter.printOutStatus(threadId, guid, "Indexer", startTimeFinal.longValue())
          logger.info("cassandraTime(s)=" + t2Total.get() / 1000000000 +
            ", processingTime[" + processingThreads + "](s)=" + timing.get() / 1000000000 +
            ", solrTime[" + luceneIndexing(0).getThreadCount + "](s)=" + luceneIndexing(0).getTiming / 1000000000 +
            ", totalTime(s)=" + (System.currentTimeMillis - timeCounter.get) / 1000 +
            ", index docs committed/in ram/ram MB=" +
            luceneIndexing(0).getCount + "/" + luceneIndexing(0).ramDocs() + "/" + (luceneIndexing(0).ramBytes() / 1024 / 1024) +
            ", mem free(Mb)=" + Runtime.getRuntime.freeMemory() / 1024 / 1024 +
            ", mem total(Mb)=" + Runtime.getRuntime.maxMemory() / 1024 / 1024 +
            ", queues (processing/lucene docs/commit batch) " + queue.size() + "/" + luceneIndexing(0).getQueueSize + "/" + luceneIndexing(0).getBatchSize)
        }

        if(Config.jmxDebugEnabled){
          JMX.updateIndexStatus(
            centralCounter.counter.get(),
            centralCounter.getAverageRecsPerSec(startTimeFinal.longValue()), //records per sec
            t2Total.get() / 1000000000, //cassandra time
            timing.get() / 1000000000, //processing time
            luceneIndexing(0).getTiming / 1000000000, //solr time
            (System.currentTimeMillis - timeCounter.get) / 1000, // totalTime
            luceneIndexing(0).getCount, //index docs committed
            luceneIndexing(0).ramDocs(), //index docs in ram
            (luceneIndexing(0).ramBytes() / 1024 / 1024), //index docs ram MB
            queue.size(), //processing queue
            luceneIndexing(0).getQueueSize, //lucene queue
            luceneIndexing(0).getBatchSize //commit batch
          )
        }
      }

      startTime.set(System.currentTimeMillis)

      t2 = System.nanoTime()

      if(maxRecordsToIndex > 0 && centralCounter.counter.get() > maxRecordsToIndex){
        logger.info("Suspending indexing. maxRecordsToIndex was reached: " + maxRecordsToIndex)
        false
      } else {
        true
      }
    }, "", "", pageSize, numThreads, true)

    logger.info("FINAL >>> cassandraTime(s)=" + t2Total.get() / 1000000000 +
      ", processingTime[" + processingThreads + "](s)=" + timing.get() / 1000000000 +
      ", solrTime[" + luceneIndexing(0).getThreadCount + "](s)=" + luceneIndexing(0).getTiming / 1000000000 +
      ", totalTime(s)=" + (System.currentTimeMillis - timeCounter.get) / 1000 +
      ", index docs committed/in ram/ram MB=" +
      luceneIndexing(0).getCount + "/" + luceneIndexing(0).ramDocs() + "/" + (luceneIndexing(0).ramBytes() / 1024 / 1024) +
      ", mem free(Mb)=" + Runtime.getRuntime.freeMemory() / 1024 / 1024 +
      ", mem total(Mb)=" + Runtime.getRuntime.maxMemory() / 1024 / 1024 +
      ", queues (processing/lucene docs/commit batch) " + queue.size() + "/" + luceneIndexing(0).getQueueSize + "/" + luceneIndexing(0).getBatchSize)

    if (csvFileWriter != null) {
      csvFileWriter.flush()
      csvFileWriter.close()
    }
    if (csvFileWriterSensitive != null) {
      csvFileWriterSensitive.flush()
      csvFileWriterSensitive.close()
    }

    //signal threads to end
    for (i <- 0 until processingThreads) {
      queue.put((null, null))
    }

    //wait for threads to end
    threads.foreach(t => t.join())

    finishTime.set(System.currentTimeMillis)
    logger.info("Total indexing time for this thread " + (finishTime.get() - start).toFloat / 60000f + " minutes. Records indexed: " + counter.intValue())
    centralCounter.setCounter(counter.intValue())

    //close and merge the lucene index parts
    if (luceneIndexing != null && !singleWriter) {
      for (i <- luceneIndexing.indices) {
        luceneIndexing(i).close(true, false)
      }
    }
  }
}