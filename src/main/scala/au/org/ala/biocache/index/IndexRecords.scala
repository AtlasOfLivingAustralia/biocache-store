package au.org.ala.biocache.index

import java.io.{File, FileWriter}
import java.util.Date
import java.util.concurrent.ArrayBlockingQueue

import au.org.ala.biocache._
import au.org.ala.biocache.cmd.{IncrementalTool, NoArgsTool, Tool}
import au.org.ala.biocache.dao.OccurrenceDAO
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.persistence.PersistenceManager
import au.org.ala.biocache.util.{FileHelper, OptionParser, StringFileWriterConsumer}
import org.slf4j.LoggerFactory

/**
  * Runnable for optimising the index.
  */
object OptimiseIndex extends NoArgsTool {

  val logger = LoggerFactory.getLogger("OptimiseIndex")

  def cmd = "optimise"

  def desc = "Optimise search index. Not for production use."

  def main(args: Array[String]): Unit = {
    proceed(args, () => Config.indexDAO.optimise)
  }
}

/**
  * Index the records to conform to the fields as defined in the schema.xml file.
  *
  * This tool is used to index small datasets or minor updates to datasets
  * in an incremental fashion if required. To complete new indexes
  * for > 1m records see the <code>BulkProcessor</code> tool.
  *
  * @author Natasha Carter
  */
object IndexRecords extends Tool with IncrementalTool {

  def cmd = "index"

  def desc = "Index records for a data resource. Not suitable for full re-indexing (>5m)"

  import FileHelper._

  val logger = LoggerFactory.getLogger("IndexRecords")
  val indexer = Config.getInstance(classOf[IndexDAO]).asInstanceOf[IndexDAO]
  val occurrenceDAO = Config.getInstance(classOf[OccurrenceDAO]).asInstanceOf[OccurrenceDAO]
  val persistenceManager = Config.getInstance(classOf[PersistenceManager]).asInstanceOf[PersistenceManager]

  def main(args: Array[String]): Unit = {
    var empty = false
    var check = false
    var startDate: Option[String] = None
    var pageSize = 1000
    var dataResourceUid: Option[String] = None
    var rowKeyFile = ""
    var threads = 1
    var test = false
    var checkRowKeyFile = true
    var abortIfNotRowKeyFile = true

    val parser = new OptionParser(help) {
      opt("empty", "empty the index first", {
        empty = true
      })
      opt("check", "check to see if the record is deleted before indexing", {
        check = true
      })
      opt("date", "date", "The earliest modification date for records to be indexed. Date in the form yyyy-mm-dd", { v: String => startDate = Some(v) })
      opt("dr", "dataResource", "The data resource to index", { v: String => dataResourceUid = Some(v) })
      intOpt("ps", "pageSize", "The page size for indexing", { v: Int => pageSize = v })
      opt("rf", "file-rowkeys-to-index", "Absolute file path to fle containing rowkeys to index", { v: String => rowKeyFile = v })
      intOpt("t", "threads", "Number of threads to index from", { v: Int => threads = v })
      opt("test", "test the speed of creating the index the minus the actual SOLR indexing costs", {
        test = true
      })
      opt("crk", "check for row key file", {
        checkRowKeyFile = true
      })
      opt("acrk", "abort if no row key file found", {
        abortIfNotRowKeyFile = true
      })
    }

    if (parser.parse(args)) {

      if (!dataResourceUid.isEmpty && checkRowKeyFile && rowKeyFile.isEmpty) {
        val (hasRowKey, retrievedRowKeyFile) = IndexRecords.hasRowKey(dataResourceUid.get)
        rowKeyFile = retrievedRowKeyFile.getOrElse("")
      }

      if (abortIfNotRowKeyFile && (rowKeyFile == "" || !(new File(rowKeyFile).exists()))) {
        logger.warn("No rowkey file was found for this index. Aborting.")
      } else if (!dataResourceUid.isEmpty && rowKeyFile.isEmpty) {
        index(dataResourceUid)
      } else {
        //delete the content of the index
        if (empty) {
          logger.info("Emptying index")
          indexer.emptyIndex
        }
        if (rowKeyFile != "") {
          if (threads == 1) {
            indexList(new File(rowKeyFile))
          } else {
            indexListThreaded(new File(rowKeyFile), threads)
          }
        }
        indexer.shutdown
      }
    }
  }

  /**
    * Index the supplied range of records.
    *
    * @param dataResource
    * @param optimise
    * @param shutdown
    * @param checkDeleted
    * @param pageSize
    * @param miscIndexProperties
    * @param callback
    * @param test
    */
  def index(dataResource: Option[String],
            optimise: Boolean = false,
            shutdown: Boolean = false,
            startDate: Option[String] = None,
            checkDeleted: Boolean = false,
            pageSize: Int = 1000,
            miscIndexProperties: Seq[String] = Array[String](),
            userProvidedTypeMiscIndexProperties: Seq[String] = Array[String](),
            callback: ObserverCallback = null,
            test: Boolean = false,
            threads: Int = 4): Unit = {

    if (dataResource.isEmpty) {
      logger.info("Starting full index")
    } else {
      logger.info("Starting to index " + dataResource.get)
    }
    indexRange(dataResource.getOrElse(""), None, checkDeleted, miscIndexProperties = miscIndexProperties, userProvidedTypeMiscIndexProperties = userProvidedTypeMiscIndexProperties, callback = callback, test = test, threads = threads)
    //index any remaining items before exiting
    indexer.finaliseIndex(optimise, shutdown)
  }

  def indexRange(dataResourceUid: String, startDate: Option[Date] = None, checkDeleted: Boolean = false,
                 pageSize: Int = 1000, miscIndexProperties: Seq[String] = Array[String](),
                 userProvidedTypeMiscIndexProperties: Seq[String] = Array[String](),
                 callback: ObserverCallback = null, test: Boolean = false, threads: Int = 4) {
    var counter = 0
    val start = System.currentTimeMillis
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis
    val csvFileWriter = if (Config.exportIndexAsCsvPath.length > 0) {
      indexer.getCsvWriter()
    } else {
      null
    }
    val csvFileWriterSensitive = if (Config.exportIndexAsCsvPathSensitive.length > 0) {
      indexer.getCsvWriter(true)
    } else {
      null
    }
    performPaging((guid, map) => {
      counter += 1
      ///convert EL and CL properties at this stage
      val shouldcommit = counter % 10000 == 0

      indexer.indexFromMap(guid,
        map,
        startDate = startDate,
        commit = shouldcommit,
        miscIndexProperties = miscIndexProperties,
        userProvidedTypeMiscIndexProperties = userProvidedTypeMiscIndexProperties,
        test = test,
        csvFileWriter = csvFileWriter,
        csvFileWriterSensitive = csvFileWriterSensitive
      )

      if (counter % pageSize == 0) {
        if (callback != null) {
          callback.progressMessage(counter)
        }
        finishTime = System.currentTimeMillis
        logger.info(counter + " >> Last key : " + guid + ", records per sec: " +
          pageSize.toFloat / (((finishTime - startTime).toFloat) / 1000f))
        startTime = System.currentTimeMillis
      }
      true
    }, dataResourceUid, checkDeleted = checkDeleted, pageSize = pageSize, threads = threads)

    if (csvFileWriter != null) {
      csvFileWriter.flush()
      csvFileWriter.close()
    }
    if (csvFileWriterSensitive != null) {
      csvFileWriterSensitive.flush()
      csvFileWriterSensitive.close()
    }

    finishTime = System.currentTimeMillis
    logger.info("Total indexing time " + ((finishTime - start).toFloat) / 1000f + " seconds")
  }

  /**
    * Page over records function
    */
  def performPaging(proc: ((String, Map[String, String]) => Boolean), dataResourceUid: String, pageSize: Int = 1000, checkDeleted: Boolean = false, threads: Int = 4) {
    val keyFile: File = Store.rowKeyFile(dataResourceUid)
    if (keyFile.exists()) {
      logger.info("Using rowKeyFile " + keyFile.getPath)
      keyFile.foreachLine(line => {
        val map = persistenceManager.get(line, "occ")
        if (!map.isEmpty && (!checkDeleted || map.getOrElse(FullRecordMapper.deletedColumn, "false").toString.equals("false"))) {
          proc(line, map.get)
        }
      })
    } else {
      logger.info("Using query for dataResourceUid")
      persistenceManager.pageOverIndexedField("occ", (guid, map) => {
        if (!map.isEmpty && (!checkDeleted || map.getOrElse(FullRecordMapper.deletedColumn, "false").equals("false"))) {
          proc(guid, map)
        }
        true
      }, "dataResourceUid", dataResourceUid, threads)
    }
  }

  /**
    * Indexes the supplied list of rowkeys
    */
  def indexList(rowKeys: List[String]) {
    var counter = 0
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis
    val csvFileWriter = if (Config.exportIndexAsCsvPath.length > 0) {
      indexer.getCsvWriter()
    } else {
      null
    }
    val csvFileWriterSensitive = if (Config.exportIndexAsCsvPathSensitive.length > 0) {
      indexer.getCsvWriter(true)
    } else {
      null
    }
    rowKeys.foreach { rowKey =>
      counter += 1
      val map = persistenceManager.get(rowKey, "occ")
      val shouldcommit = counter % 10000 == 0
      if (!map.isEmpty) indexer.indexFromMap(
        rowKey,
        map.get,
        commit = shouldcommit,
        csvFileWriter = csvFileWriter,
        csvFileWriterSensitive = csvFileWriterSensitive
      )
      if (counter % 100 == 0) {
        finishTime = System.currentTimeMillis
        logger.debug(counter + " >> Last key : " + rowKey + ", records per sec: " + 100f / (((finishTime - startTime).toFloat) / 1000f))
        startTime = System.currentTimeMillis
      }
    }
    if (csvFileWriter != null) {
      csvFileWriter.flush()
      csvFileWriter.close()
    }
    if (csvFileWriterSensitive != null) {
      csvFileWriterSensitive.flush()
      csvFileWriterSensitive.close()
    }
    indexer.finaliseIndex(false, false) //commit but don't optimise or shutdown
  }

  /**
    * Use multiple threads to run the indexing against a file of row keys.
    *
    * @param rowKeys
    * @param threads
    */
  def indexListThreaded(rowKeys: File, threads: Int) {
    val queue = new ArrayBlockingQueue[String](100)
    var ids = 0
    val csvFileWriterList: Array[FileWriter] = Array[FileWriter]()
    val pool: Array[StringFileWriterConsumer] = Array.fill(threads) {
      var counter = 0
      var startTime = System.currentTimeMillis
      var finishTime = System.currentTimeMillis
      val csvFileWriter = if (Config.exportIndexAsCsvPath.length > 0) {
        indexer.getCsvWriter()
      } else {
        null
      }
      csvFileWriterList :+ csvFileWriter
      val csvFileWriterSensitive = if (Config.exportIndexAsCsvPathSensitive.length > 0) {
        indexer.getCsvWriter(true)
      } else {
        null
      }
      csvFileWriterList :+ csvFileWriterSensitive
      indexer.init
      val p = new StringFileWriterConsumer(queue, ids, csvFileWriter, csvFileWriterSensitive, { (rowKey, csvFileWriter, csvFileWriterSensitive) =>
        counter += 1

        try {
          val map = persistenceManager.get(rowKey, "occ")
          val shouldcommit = counter % 1000 == 0
          if (!map.isEmpty) {
            indexer.indexFromMap(rowKey, map.get, commit = shouldcommit, csvFileWriter = csvFileWriter, csvFileWriterSensitive = csvFileWriterSensitive)
          }
        } catch {
          case e: Exception => logger.error("Problem indexing record with row key: '" + rowKey + "'.  ", e)
        }

        //debug counter
        if (counter % 1000 == 0) {
          finishTime = System.currentTimeMillis
          logger.info(counter + " >> Last key : " + rowKey + ", records per sec: " + 1000f / (((finishTime - startTime).toFloat) / 1000f))
          startTime = System.currentTimeMillis
        }
      })

      ids += 1
      p.start
      p
    }
    rowKeys.foreachLine(line => {
      //add to the queue
      queue.put(line.trim)
    })
    pool.foreach(t => t.shouldStop = true)
    pool.foreach(_.join)
    indexer.finaliseIndex(false, false)

    csvFileWriterList.foreach(t =>
      if (t != null) {
        t.flush()
        t.close()
      })
  }

  /**
    * Indexes the supplied list of rowKeys
    */
  def indexList(file: File, shutdown: Boolean = true) {
    logger.info("Starting the reindex by row key....")
    var counter = 0
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis
    val csvFileWriter = if (Config.exportIndexAsCsvPath.length > 0) {
      indexer.getCsvWriter()
    } else {
      null
    }
    val csvFileWriterSensitive = if (Config.exportIndexAsCsvPathSensitive.length > 0) {
      indexer.getCsvWriter()
    } else {
      null
    }

    file.foreachLine(line => {
      counter += 1
      val rowKey = if (line.head == '"' && line.last == '"') line.substring(1, line.length - 1) else line
      val map = persistenceManager.get(rowKey, "occ")
      val shouldCommit = counter % 10000 == 0
      if (!map.isEmpty) indexer.indexFromMap(rowKey, map.get, commit = shouldCommit, csvFileWriter = csvFileWriter, csvFileWriterSensitive = csvFileWriterSensitive)
      if (counter % 1000 == 0) {
        finishTime = System.currentTimeMillis
        logger.info(counter + " >> Last key : " + line + ", records per sec: " + 1000f / (((finishTime - startTime).toFloat) / 1000f))
        startTime = System.currentTimeMillis
      }
    })
    if (csvFileWriter != null) {
      csvFileWriter.flush()
      csvFileWriter.close()
    }
    if (csvFileWriterSensitive != null) {
      csvFileWriterSensitive.flush()
      csvFileWriterSensitive.close()
    }
    indexer.finaliseIndex(false, shutdown)
  }

  /**
    * Indexes the supplied list of rowKeys
    */
  def indexListOfUUIDs(file: File) {
    logger.info("Starting the reindex by UUIDs....")
    var counter = 0
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis
    val csvFileWriter = if (Config.exportIndexAsCsvPath.length > 0) {
      indexer.getCsvWriter()
    } else {
      null
    }
    val csvFileWriterSensitive = if (Config.exportIndexAsCsvPathSensitive.length > 0) {
      indexer.getCsvWriter(true)
    } else {
      null
    }

    file.foreachLine(line => {
      val uuid = line.replaceAll("\"", "")
      counter += 1

      val map = persistenceManager.getByIndex(uuid, "occ", "uuid")
      val shouldCommit = counter % 10000 == 0

      if (!map.isEmpty) indexer.indexFromMap(uuid, map.get, commit = shouldCommit, csvFileWriter = csvFileWriter, csvFileWriterSensitive = csvFileWriterSensitive)
      if (counter % 1000 == 0) {
        finishTime = System.currentTimeMillis
        logger.info(counter + " >> Last key : " + line + ", records per sec: " + 1000f / (((finishTime - startTime).toFloat) / 1000f))
        startTime = System.currentTimeMillis
      }
    })
    if (csvFileWriter != null) {
      csvFileWriter.flush()
      csvFileWriter.close()
    }
    if (csvFileWriterSensitive != null) {
      csvFileWriterSensitive.flush()
      csvFileWriterSensitive.close()
    }

    logger.info("Finalising index.....")
    indexer.finaliseIndex(false, true)
    logger.info("Finalised index.")
  }
}