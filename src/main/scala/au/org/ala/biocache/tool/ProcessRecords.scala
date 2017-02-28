package au.org.ala.biocache.tool

import java.io.File
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import au.org.ala.biocache._
import au.org.ala.biocache.cmd.{IncrementalTool, Tool}
import au.org.ala.biocache.model.FullRecord
import au.org.ala.biocache.processor.RecordProcessor
import au.org.ala.biocache.util.{FileHelper, OptionParser}
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

import scala.actors.Actor
import scala.actors.threadpool.LinkedBlockingQueue
import scala.collection.mutable

object ProcessAll extends Tool {

  def cmd = "process-all"
  def desc = "Process all records"

  def main(args:Array[String]){

    var threads:Int = 4
    val parser = new OptionParser(help) {
      intOpt("t", "thread", "The number of threads to use", {v:Int => threads = v } )
    }
    if(parser.parse(args)){
      ProcessRecords.processRecords(threads, None, None)
    }
  }
}

/**
 * A simple threaded implementation of the processing.
 */
object ProcessRecords extends Tool with IncrementalTool {

  import FileHelper._

  def cmd = "process"
  def desc = "Process records (geospatial, taxonomy)"

  val occurrenceDAO = Config.occurrenceDAO
  val persistenceManager = Config.persistenceManager
  val logger = LoggerFactory.getLogger("ProcessRecords")

  def main(args : Array[String]) : Unit = {

    logger.info("Starting processing...")
    var threads:Int = 4
    var startUuid:Option[String] = None
    var endUuid:Option[String] = None
    var checkDeleted = false
    var dataResourceUid:Option[String] = None
    var checkRowKeyFile = false
    var rowKeyFile = ""
    var abortIfNotRowKeyFile = false

    val parser = new OptionParser(help) {
      intOpt("t", "thread", "The number of threads to use", {v:Int => threads = v } )
      opt("s", "start","The record to start with", {v:String => startUuid = Some(v)})
      opt("e", "end","The record to end with", {v:String => endUuid = Some(v)})
      opt("dr", "resource", "The data resource to process", {v:String => dataResourceUid = Some(v)})
      booleanOpt("cd", "checkDeleted", "Check deleted records", {v:Boolean => checkDeleted = v})
      opt("crk", "check for row key file",{ checkRowKeyFile = true })
      opt("acrk", "abort if no row key file found",{ abortIfNotRowKeyFile = true })
    }

    if(parser.parse(args)){

      if(!dataResourceUid.isEmpty && checkRowKeyFile){
        val (hasRowKey, retrievedRowKeyFile) = ProcessRecords.hasRowKey(dataResourceUid.get)
        rowKeyFile = retrievedRowKeyFile.getOrElse("")
      }

      if(abortIfNotRowKeyFile && (rowKeyFile == "" || !(new File(rowKeyFile).exists()))){
        logger.warn("No rowkey file was found for this processing. Aborting.")
      } else {
        if (rowKeyFile != "") {
          //process the row key file
          processRecords(new java.io.File(rowKeyFile), threads, None)
        } else {
          logger.info("Processing " + dataResourceUid.getOrElse("") + " from " + startUuid.getOrElse("*") + " to " + endUuid.getOrElse("*") + " with " + threads + " actors")
          processRecords(threads, startUuid, dataResourceUid, checkDeleted, lastKey = endUuid)
        }
      }
    }
  }

  /**
   * Processes the supplied row keys in a Thread
   */
  def processRecords(file:File, threads: Int, startUuid:Option[String]) : Unit = {
    processRecords0(file, threads, startUuid)
  }

  /**
    * Process the records using the supplied number of threads
    */
  def processRecords(threads: Int, firstKey:Option[String], dr: Option[String], checkDeleted:Boolean=false,
                     callback:ObserverCallback = null, lastKey:Option[String]=None): Unit = {
    processRecords0(null, threads, firstKey, dr, checkDeleted, callback, lastKey)
  }

  /**
    * Processes a list of records
    */
  def processRecords(rowKeys:List[String]): Unit ={
    val file:File = File.createTempFile("uuids","")
    FileUtils.writeStringToFile(file, rowKeys.mkString("\n"))

    processRecords(file, 4, None)
  }


  def processRecords0(file:File, threads: Int, startUuid:Option[String], dr: Option[String] = null,
                     checkDeleted:Boolean=false, callback:ObserverCallback = null, lastKey:Option[String]=None) : Unit = {
    var buff = new LinkedBlockingQueue[Object](1000)
    var writeBuffer = new LinkedBlockingQueue[Object](threads)
    var ids = 0
    val pool = Array.fill(threads){ val p = new Consumer(Actor.self,ids,buff, writeBuffer); ids +=1; p.start; p }
    val writer = {val p = new ProcessedBatchWriter(writeBuffer, callback).start; p}
    logger.info("Starting to process a list of records...")
    val start = System.currentTimeMillis
    val startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis

    //use this variable to evenly distribute the actors work load
    val count:Int = {
      if (file != null) readUuidList(file, startUuid, buff)
      else readFromDB(startUuid, dr, checkDeleted, lastKey, buff)
    }

    logger.info("Finished reading.")

    //We can't shutdown the persistence manager until all of the Actors have completed their work
    for(p <- pool) buff.put("exit")
    for(p <- pool)
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

  def readFromDB( firstKey:Option[String], dr: Option[String],
                  checkDeleted:Boolean=false, lastKey:Option[String]=None,
                  buffer: LinkedBlockingQueue[Object]): Int = {
    val endUuid = if (lastKey.isDefined) lastKey.get else if(dr.isEmpty) "" else dr.get +"|~"

    val startUuid = {
      if(!dr.isEmpty) {
        dr.get +"|"
      } else {
        firstKey.getOrElse("")
      }
    }

    logger.info("Starting with " + startUuid +" ending with " + endUuid)
    val start = System.currentTimeMillis
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis

    logger.debug("Initialised actors...")

    var count = 0
    var guid = "";
    //use this variable to evenly distribute the actors work load
    var batches = 0

    performPaging(rawAndProcessed => {
      if(guid == "") logger.info("First rowKey processed: " + rawAndProcessed.get._1.rowKey)
      guid = rawAndProcessed.get._1.rowKey
      count += 1

      //we want to add the record to the buffer whether or not we send them to the actor
      //add it to the buffer isnt a deleted record
      if (!rawAndProcessed.isEmpty && !rawAndProcessed.get._1.deleted){
        buffer.put(rawAndProcessed.get)
      }

      //debug counter
      if (count % 1000 == 0) {
        logger.info("records read: " + count)
      }
      true //indicate to continue
    }, startUuid, endUuid)

    count
  }

  def readUuidList(file: File, startUuid:Option[String], buffer: LinkedBlockingQueue[Object]): Int = {
    val startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis
    var count = 0
    logger.debug("Initialised actors...")
    file.foreachLine(line => {
      count += 1
      if(startUuid.isEmpty || startUuid.get == line) {
        val record = occurrenceDAO.getRawProcessedByRowKey(line)
        if(!record.isEmpty){
          buffer.put(line)
        }
      }

      if (count % 1000 == 0) {
        finishTime = System.currentTimeMillis
        logger.info("records read: " + count)
      }
    })

    count
  }

  def performPaging(proc: (Option[(FullRecord, FullRecord)] => Boolean), startKey:String="", endKey:String="", pageSize: Int = 1000){
    occurrenceDAO.pageOverRawProcessed(rawAndProcessed => {
      proc(rawAndProcessed)
    }, startKey, endKey, pageSize)
  }

  def printTimings(pool:Array[Consumer]): Unit = {
    val processorTimings:mutable.Map[String, Long] = scala.collection.mutable.Map()
    for(p <- pool) {
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
  private var count:AtomicInteger = new AtomicInteger(0)
  private var totalTime:AtomicLong = new AtomicLong(0)
  private var lastTime:AtomicLong = new AtomicLong(0)
}

class Consumer (master:Actor, val id:Int, val buffer:LinkedBlockingQueue[Object],
                val writeBuffer: LinkedBlockingQueue[Object], val firstLoad:Boolean=false)  extends Actor  {

  val logger = LoggerFactory.getLogger("Consumer")

  logger.debug("Initialising thread: " + id)
  val processor = new RecordProcessor
  val occurrenceDAO = Config.occurrenceDAO

  var batches = new scala.collection.mutable.ListBuffer[Map[String, Object]]
  val batchSize = 200

  def act {
    logger.info("In thread: "+id)
    while (true) {
      buffer.take() match {

        case rawAndProcessed: (FullRecord, FullRecord) => {
          val (raw, processed) = rawAndProcessed

          val startTime = System.currentTimeMillis
          batches += processor.processRecord(raw, processed, firstLoad)
          Consumer.totalTime.addAndGet(System.currentTimeMillis() - startTime)

          if (batches.length == batchSize) {
            writeBuffer.put(batches.toList)
            batches = new scala.collection.mutable.ListBuffer[Map[String, Object]]
          }

          val c: Int = Consumer.count.addAndGet(1)
          if (c % 1000 == 0) {
            val t: Long = Consumer.totalTime.get()
            logger.info("processed " + c + " records : last uuid " + raw.rowKey + " : "
               + (1000f / ((t - Consumer.lastTime.get()).toFloat / 60000f) ) + " records processed per minute")
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
class ProcessedBatchWriter (val writeBuffer: LinkedBlockingQueue[Object], val callback: ObserverCallback)  extends Actor  {

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
    while (true) {
      writeBuffer.take() match {

        case batch: List[Map[String, Object]] => {
            startTime = System.currentTimeMillis
            processor.writeProcessBatch(batch)
            writeTime += System.currentTimeMillis - startTime
            writeTimeTotal += System.currentTimeMillis - startTime

            recordCount += batch.size
            count += batch.size
            if (count > nextPos) {
              finishTime = System.currentTimeMillis

              logger.info(count
                + " >> writing records per sec: " + recordCount / (((writeTime).toFloat) / 1000f)
                + ", time taken to write "+recordCount+" records: " + (writeTime).toFloat / 1000f
                + ", total time: "+ (writeTimeTotal).toFloat / 60000f +" minutes"
              )
              writeTime = 0
              recordCount = 0

              nextPos = count + 1000
              if(callback != null) {
                callback.progressMessage(count)
              }
            }
        }

        case s: Object => {
          if (s.equals("exit")) {
            logger.debug("Killing (Actor.act) ProcessedBufferWriter thread")
            if (callback != null) {
              callback.progressMessage(count)
            }

            logger.info(count
              + " >> Finished writing, total records: " + count
              + ", total time: " + (finishTime - firstTime).toFloat / 60000f + " minutes"
            )

            exit()
          }
        }
      }
    }
  }
}