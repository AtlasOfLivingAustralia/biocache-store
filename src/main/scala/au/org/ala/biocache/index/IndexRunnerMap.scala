package au.org.ala.biocache.index

import java.io.File
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong

import au.org.ala.biocache._
import au.org.ala.biocache.index.lucene.LuceneIndexing
import au.org.ala.biocache.util.JMX
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class IndexRunnerMap(centralCounter: Counter,
                     confDirPath: String, pageSize: Int = 200,
                     luceneIndexing: Seq[LuceneIndexing] = null,
                     processingThreads: Integer = 1,
                     processorBufferSize: Integer = 100,
                     singleWriter: Boolean = false,
                     test: Boolean = false,
                     numThreads: Int = 2) extends Runnable {

  val logger: Logger = LoggerFactory.getLogger("IndexRunner")

  val startTimeFinal: Long = System.currentTimeMillis()

  val directoryList = new java.util.ArrayList[File]

  val timing = new AtomicLong(0)

  val threadId = 0

  def run() {

    //solr-create/thread-0/conf
    val newIndexDir = new File(confDirPath)

    val indexer = new SolrIndexDAO(newIndexDir.getParentFile.getParent, Config.excludeSensitiveValuesFor, Config.extraMiscFields)

    var counter = 0
    val start = System.currentTimeMillis
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis

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

    val queue: LinkedBlockingQueue[(String, Map[String, String])] = new LinkedBlockingQueue[(String, Map[String, String])](processorBufferSize)

    val threads = mutable.ArrayBuffer[ProcessThread]()

    val lock: Array[Object] = new Array[Object](luceneIndexing.size)
    for (i <- luceneIndexing.indices) {
      lock(i) = new Object()
    }

    if (processingThreads > 0 && luceneIndexing != null) {
      for (i <- 0 until processingThreads) {
        var t = new ProcessThread()

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

      override def run() {

        // Specify the SOLR config to use
        indexer.solrConfigPath = newIndexDir.getAbsolutePath + "/solrconfig.xml"
        var continue = true
        while (continue) {
          val m: (String, Map[String, String]) = queue.take()
          if (m._1 == null) {
            continue = false
          } else {
            try {
              val t1 = System.nanoTime()

              val t2 = indexer.indexFromMapNew(m._1, m._2,
                docBuilder = luceneIndexer.getDocBuilder,
                lock = lock,
                test = test)

              timing.addAndGet(System.nanoTime() - t1 - t2)

            } catch {
              case e: InterruptedException => throw e
              case e: Exception => logger.error("guid:" + m._1 + ", " + e.getMessage)
            }
          }
          recordsProcessed = recordsProcessed + 1
        }
      }
    }

    //page through and create and index for this range
    val t2Total = new AtomicLong(0L)
    var t2 = System.nanoTime()
    var uuidIdx = -1
    Config.persistenceManager.pageOverLocal("occ", (guid, map, _) => {
      t2Total.addAndGet(System.nanoTime() - t2)

      counter += 1
      //ignore the record if it has the guid that is the startKey this is because it will be indexed last by the previous thread.
      try {
        val uuid = map.getOrElse("uuid", "")
        if (!StringUtils.isEmpty(uuid)) {
          val t1 = System.nanoTime()
          var t2 = 0L

          if (processingThreads > 0) {
            queue.put((uuid, map))
          } else {
            t2 = indexer.indexFromMapNew(uuid, map,
              docBuilder = luceneIndexing(0).getDocBuilder,
              lock = lock(0),
              test = test)
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

      if (counter % pageSize * 10 == 0 && counter > 0) {
        centralCounter.addToCounter(pageSize)
        finishTime = System.currentTimeMillis
        centralCounter.printOutStatus(threadId, guid, "Indexer", startTimeFinal)


        logger.info("cassandraTime(s)=" + t2Total.get() / 1000000000 +
          ", processingTime[" + processingThreads + "](s)=" + timing.get() / 1000000000 +
          ", solrTime[" + luceneIndexing(0).getThreadCount + "](s)=" + luceneIndexing(0).getTiming / 1000000000 +
          ", index docs committed/in ram/ram MB=" +
          luceneIndexing(0).getCount + "/" + luceneIndexing(0).ramDocs() + "/" + (luceneIndexing(0).ramBytes() / 1024 / 1024) +
          ", mem free(Mb)=" + Runtime.getRuntime.freeMemory() / 1024 / 1024 +
          ", mem total(Mb)=" + Runtime.getRuntime.maxMemory() / 1024 / 1024 +
          ", queues (processing/lucene docs/commit batch) " + queue.size() + "/" + luceneIndexing(0).getQueueSize + "/" + luceneIndexing(0).getBatchSize)

        if(Config.jmxDebugEnabled){
          JMX.updateIndexStatus(
            centralCounter.counter,
            centralCounter.getAverageRecsPerSec(startTimeFinal), //records per sec
            t2Total.get() / 1000000000, //cassandra time
            timing.get() / 1000000000, //processing time
            luceneIndexing(0).getTiming / 1000000000, //solr time
            0, // totalTime
            luceneIndexing(0).getCount, //index docs committed
            luceneIndexing(0).ramDocs(), //index docs in ram
            (luceneIndexing(0).ramBytes() / 1024 / 1024), //index docs ram MB
            queue.size(), //processing queue
            luceneIndexing(0).getQueueSize, //lucene queue
            luceneIndexing(0).getBatchSize //commit batch
          )
        }
      }

      startTime = System.currentTimeMillis

      t2 = System.nanoTime()

      //counter < 2000
      true
    }, numThreads, Array())

    //final log entry
    centralCounter.printOutStatus(threadId, "", "Indexer", startTimeFinal)

    logger.info("FINAL >>> cassandraTime(s)=" + t2Total.get() / 1000000000 +
      ", processingTime[" + processingThreads + "](s)=" + timing.get() / 1000000000 +
      ", solrTime[" + luceneIndexing(0).getThreadCount + "](s)=" + luceneIndexing(0).getTiming / 1000000000 +
      ", index docs committed/in ram/ram MB=" +
      luceneIndexing(0).getCount + "/" + luceneIndexing(0).ramDocs() + "/" + (luceneIndexing(0).ramBytes() / 1024 / 1024) +
      ", mem free(Mb)=" + Runtime.getRuntime.freeMemory() / 1024 / 1024 +
      ", mem total(Mb)=" + Runtime.getRuntime.maxMemory() / 1024 / 1024 +
      ", queues (processing/lucene docs/commit batch) " + queue.size() + "/" + luceneIndexing(0).getQueueSize + "/" + luceneIndexing(0).getBatchSize)

    if (csvFileWriter != null) {
      csvFileWriter.flush();
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

    finishTime = System.currentTimeMillis
    logger.info("Total indexing time for this thread " + (finishTime - start).toFloat / 60000f + " minutes.")

    //close and merge the lucene index parts
    if (luceneIndexing != null && !singleWriter) {
      for (i <- luceneIndexing.indices) {
        luceneIndexing(i).close(true, false)
      }
    }
  }
}