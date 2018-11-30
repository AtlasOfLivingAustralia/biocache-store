package au.org.ala.biocache.tool

import au.org.ala.biocache._
import java.io.File
import org.slf4j.LoggerFactory
import au.org.ala.biocache.util.{StringConsumer, OptionParser, FileHelper}
import au.org.ala.biocache.cmd.{IncrementalTool, Tool}
import au.org.ala.biocache.processor.RecordProcessor
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicInteger
import java.util.UUID

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
    var checkDeleted = false
    var dataResourceUid:Option[String] = None
    var checkRowKeyFile = true
    var rowKeyFile = ""
    var abortIfNotRowKeyFile = true

    val parser = new OptionParser(help) {
      intOpt("t", "thread", "The number of threads to use", {v:Int => threads = v } )
      opt("dr", "resource", "The data resource to process", {v:String => dataResourceUid = Some(v)})
      opt("rf", "file-rowkeys-to-index", "Absolute file path to fle containing rowkeys to index", { v: String => rowKeyFile = v })
      booleanOpt("cd", "checkDeleted", "Check deleted records", {v:Boolean => checkDeleted = v})
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
          processFileOfRowKeys(new java.io.File(rowKeyFile), threads)
        } else {
          logger.info("rowkey file not found")
        }
      }
    }
  }

  def processRecords(dataResourceUid:String, threads: Int): Unit = {

    val (hasRowKey, retrievedRowKeyFile) = ProcessRecords.hasRowKey(dataResourceUid)

    if(retrievedRowKeyFile.isEmpty || !new File(retrievedRowKeyFile.get).exists()){
      logger.warn("No rowkey file was found for this processing. Aborting.")
    } else {
        //process the row key file
        processFileOfRowKeys(new java.io.File(retrievedRowKeyFile.get), threads)
    }
  }

  /**
    * Process a set of records with keys in the supplied file
    * @param rowkeys
    */
  def processRowKeys(rowkeys:List[String]) {

    val queue = rowkeys
    val counter = new AtomicLong(0)
    val recordProcessor = new RecordProcessor
    rowkeys.foreach { guid =>
      if (!guid.trim().isEmpty()) {
        counter.incrementAndGet()
        val rawProcessed = Config.occurrenceDAO.getRawProcessedByRowKey(guid)
        if (!rawProcessed.isEmpty) {
          val rp = rawProcessed.get
          recordProcessor.processRecord(rp(0), rp(1))
        }
      }
    }

    logger.info("Total records processed: " + counter.get())
  }

  /**
   * Process a set of records with keys in the supplied file
   * @param file
   * @param threads
   */
  def processFileOfRowKeys(file: java.io.File, threads: Int) {
    val queue = new ArrayBlockingQueue[String](1000)
    val ids = new AtomicInteger(0)
    val counter = new AtomicLong(0)
    val recordProcessor = new RecordProcessor
    val startTime = new AtomicLong(System.currentTimeMillis)
    val finishTime = new AtomicLong(0)
    // Sentinel that will not match any of the raw GUIDs in the file and signals the consumers to return
    val sentinel = UUID.randomUUID().toString() + System.currentTimeMillis()
    val pool: Array[StringConsumer] = Array.fill(threads) {

      val p = new StringConsumer(queue, ids.incrementAndGet(), sentinel, { guid =>
        if (!guid.trim().isEmpty()) {
          var lastCounter = counter.incrementAndGet();
          val rawProcessed = Config.occurrenceDAO.getRawProcessedByRowKey(guid)
          if (!rawProcessed.isEmpty) {
            val rp = rawProcessed.get
            recordProcessor.processRecord(rp(0), rp(1))

            //debug counter
            if (lastCounter % 1000 == 0) {
              finishTime.set(System.currentTimeMillis)
              logger.info(lastCounter + " >> Last key : " + rp(0).rowKey + ", records per sec: " + 1000f / (((finishTime.get() - startTime.get()).toFloat) / 1000f))
              startTime.set(System.currentTimeMillis)
            }
          }
        }
      })
      p.start
      p
    }

    file.foreachLine(line => 
      if (!line.trim().isEmpty()) {
        queue.put(line.trim)
      }
    )
    for (i <- 1 to threads) {
      queue.put(sentinel)
    }
    pool.foreach(_.join)

    logger.info("Total records processed: " + counter.get())
  }
}
