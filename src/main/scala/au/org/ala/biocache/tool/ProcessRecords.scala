package au.org.ala.biocache.tool

import java.io.File
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import au.org.ala.biocache._
import au.org.ala.biocache.cmd.{IncrementalTool, Tool}
import au.org.ala.biocache.model.FullRecord
import au.org.ala.biocache.processor.{Processors, RecordProcessor}
import au.org.ala.biocache.util.{FileHelper, OptionParser}
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

import scala.actors.Actor
import scala.actors.threadpool.LinkedBlockingQueue
import scala.collection.mutable

object ProcessAll extends Tool {

  def cmd = "process-all"

  def desc = "Process all records"

  def main(args: Array[String]) {

    var threads: Int = 4
    var processors: Option[String] = None
    val parser = new OptionParser(help) {
      intOpt("t", "thread", "The number of threads to use", { v: Int => threads = v })
      opt("p", "processors", "Comma separated list of processors to run. One or more of: " +
        Processors.processorMap.values.collect({ case it => it.getName }).mkString(","), { v: String => processors = Some(v) })
    }
    if (parser.parse(args)) {
      ProcessRecords.processRecords0(threads = threads, processors = processors)
    }
  }
}

/**
  * A simple threaded implementation of the processing.
  */
object ProcessRecords extends Tool with IncrementalTool {
  def processRecords(rowKeys: List[String]) = {
    processRecords0(makeRowKeyFile(rowKeys))
  }

  import FileHelper._

  def cmd = "process"

  def desc = "Process records (geospatial, taxonomy)"

  val occurrenceDAO = Config.occurrenceDAO
  val persistenceManager = Config.persistenceManager
  val logger = LoggerFactory.getLogger("ProcessRecords")

  def main(args: Array[String]): Unit = {

    logger.info("Starting processing...")
    var threads: Int = 4
    var startUuid: Option[String] = None
    var endUuid: Option[String] = None
    var checkDeleted = false
    var dataResourceUid: Option[String] = None
    var rowKeys: Array[String] = Array()
    var checkRowKeyFile = true
    var rowKeyFile = ""
    var abortIfNotRowKeyFile = false
    var processors: Option[String] = None

    val parser = new OptionParser(help) {
      intOpt("t", "thread", "The number of threads to use", { v: Int => threads = v })
      opt("s", "start", "The record to start with in the row key file", { v: String => startUuid = Some(v) })
      opt("e", "end", "The record to end with in the row key file", { v: String => endUuid = Some(v) })
      opt("dr", "resource", "The data resource to process", { v: String => dataResourceUid = Some(v) })
      booleanOpt("cd", "checkDeleted", "Check deleted records", { v: Boolean => checkDeleted = v })
      opt("rk", "comma separated list of rowkeys to process.", { v: String => rowKeys = v.split(",") })
      opt("rf", "path to row key file.", { v: String => rowKeyFile = v })
      opt("crk", "check for row key file, " + Config.tmpWorkDir + "/row_key_<dr>.csv", {
        checkRowKeyFile = true
      })
      opt("acrk", "abort if no row key file found", {
        abortIfNotRowKeyFile = true
      })
      opt("p", "processors", "comma separated list of processors to run. One or more of: " +
        Processors.processorMap.values.collect({ case it => it.getName }).mkString(","), { v: String => processors = Some(v) })
    }

    if (parser.parse(args)) {

      if (!dataResourceUid.isEmpty && checkRowKeyFile && rowKeyFile.isEmpty) {
        val (hasRowKey, retrievedRowKeyFile) = ProcessRecords.hasRowKey(dataResourceUid.get)
        rowKeyFile = retrievedRowKeyFile.getOrElse("")
      }

      if (abortIfNotRowKeyFile && (rowKeyFile == "" || !new File(rowKeyFile).exists)) {
        logger.warn("No rowkey file was found for this processing. Aborting.")
      } else {
        var file: File = null
        if (!rowKeys.isEmpty && rowKeyFile.isEmpty) {
          file = makeRowKeyFile(rowKeys.toList)
        } else if (rowKeyFile != "") {
          file = new java.io.File(rowKeyFile)
        }

        processRecords0(file, dataResourceUid, threads, startUuid, endUuid, checkDeleted, processors, null, None)
      }
    }
  }

  def makeRowKeyFile(rowKeys: List[String]): File = {
    val file: File = File.createTempFile("uuids", "")
    FileUtils.writeStringToFile(file, rowKeys.mkString("\n"))
    file
  }

  def processRecords0(file: File = null, dr: Option[String] = null, threads: Int = 4, startUuid: Option[String] = None, endUuid: Option[String] = None,
                      checkDeleted: Boolean = false, processors: Option[String] = None, callback: ObserverCallback = null,
                      lastKey: Option[String] = None): Unit = {
    val buff = new LinkedBlockingQueue[Object](1000)
    val writeBuffer = new LinkedBlockingQueue[Object](threads)
    var ids = 0
    val writer = {
      val p = new ProcessedBatchWriter(writeBuffer, callback)
      p.start
      p
    }
    val pool = Array.fill(threads) {
      val p = new Consumer(Actor.self, ids, buff, writeBuffer, processors = processors)
      ids += 1
      p.start
      p
    }
    logger.info("Starting to process a list of records...")

    //use this variable to evenly distribute the actors work load
    val count: Int = {
      if (file != null) readUuidList(file, startUuid, buff)
      else readFromDB(startUuid, dr, checkDeleted, lastKey, threads, buff)
    }

    logger.info("Finished reading.")

    //We can't shutdown the persistence manager until all of the Actors have completed their work
    for (p <- pool) buff.put("exit")
    for (p <- pool)
      while (p.getState != Actor.State.Terminated)
        Thread.sleep(50)

    printTimings(pool)
    logger.info("Finished processing.")
    //wait for writer to finish
    writeBuffer.put("exit")
    while (writer.getState != Actor.State.Terminated)
      Thread.sleep(50)

    logger.info("Finished writing.")
  }

  def readFromDB(firstKey: Option[String], dr: Option[String],
                 checkDeleted: Boolean = false, lastKey: Option[String] = None, threads: Int = 1,
                 buffer: LinkedBlockingQueue[Object]): Int = {

    logger.info("Processing " + dr.getOrElse("all records"))

    logger.debug("Initialised actors...")

    var count = 0
    var guid = ""

    var start = firstKey.isEmpty
    performPaging(rawAndProcessed => {
      if (guid == "") logger.info("First rowkey processed: " + rawAndProcessed.get._1.rowKey)
      guid = rawAndProcessed.get._1.rowKey
      if (!start && firstKey.isDefined) {
        start = firstKey.get == guid
      }
      if (start) {
        count += 1

        //we want to add the record to the buffer whether or not we send them to the actor
        //add it to the buffer isnt a deleted record
        if (rawAndProcessed.isDefined && (!checkDeleted || !rawAndProcessed.get._1.deleted)) {
          buffer.put(rawAndProcessed.get)
        }

        //debug counter
        if (count % 1000 == 0) {
          logger.info("records read: " + count)
        }
      }

      lastKey.isEmpty || guid != lastKey.get //indicate to continue
    }, dr.getOrElse(""), threads = threads)

    count
  }

  def readUuidList(file: File, startUuid: Option[String], buffer: LinkedBlockingQueue[Object]): Int = {
    var count = 0
    logger.debug("Initialised actors...")
    file.foreachLine(line => {
      count += 1
      if (startUuid.isEmpty || startUuid.get == line) {
        val record = occurrenceDAO.getRawProcessedByRowKey(line)
        if (record.isDefined) {
          buffer.put((record.get(0), record.get(1)))
        }
      }

      if (count % 1000 == 0) {
        logger.info("records read: " + count)
      }
    })

    count
  }

  def performPaging(proc: (Option[(FullRecord, FullRecord)] => Boolean), dataResourceUid: String = "", pageSize: Int = 1000, threads: Int = 1) {
    occurrenceDAO.pageOverRawProcessed(rawAndProcessed => {
      proc(rawAndProcessed)
    }, dataResourceUid, pageSize, threads)
  }

  def printTimings(pool: Array[Consumer]): Unit = {
    val processorTimings: mutable.Map[String, Long] = scala.collection.mutable.Map()
    for (p <- pool) {
      p.processor.processTimings.foreach { e =>
        processorTimings += (e._1 -> (e._2 + processorTimings.getOrElse(e._1, 0L)))
      }
    }

    val timings = new StringBuilder()
    timings.append("time for each processor (ms):")
    processorTimings.foreach { e => timings.append(e._1 + "=" + e._2 + "; ") }
    logger.info(timings.toString)
  }
}

/**
  * A consumer actor asks for new work.
  */
object Consumer {
  private var count: AtomicInteger = new AtomicInteger(0)
  private var totalTime: AtomicLong = new AtomicLong(0)
  private var lastTime: AtomicLong = new AtomicLong(0)
}

class Consumer(master: Actor, val id: Int, val buffer: LinkedBlockingQueue[Object],
               val writeBuffer: LinkedBlockingQueue[Object], val firstLoad: Boolean = false,
               val processors: Option[String] = None) extends Actor {

  val logger = LoggerFactory.getLogger("Consumer")

  logger.debug("Initialising thread: " + id)
  val processor = new RecordProcessor
  val occurrenceDAO = Config.occurrenceDAO

  var batches = new scala.collection.mutable.ListBuffer[Map[String, Object]]
  val batchSize = 200

  def act {
    while (true) {
      buffer.take() match {

        case rawAndProcessed: (FullRecord, FullRecord) => {
          val (raw, processed) = rawAndProcessed

          val startTime = System.nanoTime()
          batches += processor.processRecord(raw, processed, firstLoad, processors = processors)
          Consumer.totalTime.addAndGet(System.nanoTime() - startTime)

          if (batches.length == batchSize) {
            writeBuffer.put(batches.toList)
            batches = new scala.collection.mutable.ListBuffer[Map[String, Object]]
          }

          val c: Int = Consumer.count.addAndGet(1)
          if (c % 1000 == 0) {
            val t: Long = Consumer.totalTime.get()
            logger.info("processed " + c + " records : last uuid " + raw.rowKey + " : "
              + (1000f / ((t - Consumer.lastTime.get()).toFloat / 60000f / 1000000f)) + " records processed per minute")

            Consumer.lastTime.set(t)
          }
        }

        case s: Object => {
          if (s == "exit") {
            if (!batches.isEmpty) {
              writeBuffer.put(batches.toList)
            }
            logger.debug("Killing (Actor.act) thread: " + id)
            exit()
          }
        }
      }
    }
  }
}

/**
  * A writer actor to do db writes
  */
class ProcessedBatchWriter(val writeBuffer: LinkedBlockingQueue[Object], val callback: ObserverCallback) extends Actor {

  val logger = LoggerFactory.getLogger("ProcessedBatchWriter")

  logger.debug("Initialising ProcessedBufferWriter thread")
  val processor = new RecordProcessor
  var count = 0
  var nextPos = 1000

  val firstTime = System.currentTimeMillis
  var startTime = System.currentTimeMillis
  var finishTime = System.currentTimeMillis
  var writeTime = 0L
  var writeTimeTotal = 0L
  var recordCount = 0L

  def act {
    startTime = System.currentTimeMillis
    while (true) {
      writeBuffer.take() match {
        case batch: List[Map[String, Object]] => {
          try {
            processor.writeProcessBatch(batch)

            recordCount += batch.size
            count += batch.size
            if (count > nextPos) {

              logger.info(count
                + " >> writing records per sec (average): " + count / (((System.currentTimeMillis - firstTime).toFloat) / 1000f)
                + ", total time " + (System.currentTimeMillis - firstTime).toFloat / 60000f
                + "minutes"
              )
              writeTime = 0
              recordCount = 0

              nextPos = count + 1000
              if (callback != null) {
                callback.progressMessage(count)
              }
            }
          } catch {
            case e: Exception => logger.error("failed to write batch. adding to queue to retry writing: " + e.getMessage, e)
              try {
                writeBuffer.put(batch)
              }
          }
        }

        case s: Object => {
          if (s.equals("exit")) {
            logger.info("Killing (Actor.act) ProcessedBufferWriter thread")
            if (callback != null) {
              callback.progressMessage(count)
            }

            logger.info(count
              + " >> Finished writing, total records: " + count
              + ", total time spent writing: " + (System.currentTimeMillis - firstTime).toFloat / 60000f + " minutes"
            )

            exit()
          }
        }
      }
    }
  }
}