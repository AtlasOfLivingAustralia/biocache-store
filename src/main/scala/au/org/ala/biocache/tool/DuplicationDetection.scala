package au.org.ala.biocache.tool

import java.io.{File, FileReader, FileWriter}
import java.util.concurrent.ArrayBlockingQueue

import au.com.bytecode.opencsv.CSVReader
import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.export.{ExportAllSpatialSpecies, ExportByFacetQuery}
import au.org.ala.biocache.model.{DuplicateRecordDetails, DuplicationTypes, QualityAssertion}
import au.org.ala.biocache.util.{FileHelper, OptionParser, StringConsumer}
import au.org.ala.biocache.vocab.AssertionCodes
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.DateUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.UUID

/**
  * Companion object for the duplicate detection class.
  * Duplication detection is only possible if latitude and longitude are provided with the record.
  *
  * The running of the duplicate detection and the load of the results is split into two
  * for operational reasons.
  *
  * The algorithm runs thus:
  *
  * Step 1:
  * a) Get a distinct list of species lsids that have been matched
  * b) Get a distinct list of subspecies lsids (without species lsids) that have been matched
  *
  * Step 2
  * Break down all the records into groups based on the occurrence year - all null year (thus date) records will be
  * handled together.
  *
  * Step 3
  * a) within the year groupings break down into groups based on months - all nulls will be placed together
  * b) within month groupings break down into groups based on event date - all nulls will be placed together
  *
  * Step 4
  * a) With the smallest grained group from Step 3 group all the similar "collectors" together null or unknown collectors
  * will be handled together
  * b) With the collector groups determine which of the records have the same coordinates (ignoring differences in
  * precision)
  */
object DuplicationDetection extends Tool {

  import FileHelper._

  val logger = LoggerFactory.getLogger("DuplicateDetection")

  var workingTmpDir = Config.tmpWorkDir + "/duplication/"

  def cmd = "duplicate-detection"

  def desc = "Detects duplication based on a matched species and updates the database with details."

  def main(args: Array[String]) {

    var all = false
    var exist = false
    var guid: Option[String] = None
    var speciesFile: Option[String] = None
    var threads = 4
    var cleanup = false
    var load = false
    var incremental = false
    var removeObsoleteData = false
    var offlineDir = Config.tmpWorkDir +"/exports"

    //Options to perform on all "species", select species, use existing file or download
    val parser = new OptionParser(help) {
      opt("all", "detect duplicates for all species", {
        all = true
      })
      opt("g", "guid", "A single record GUID to test for duplications", {
        v: String => guid = Some(v)
      })
      opt("exist", "use existing occurrence dumps", {
        exist = true
      })
      opt("inc", "perform an incremental duplication detection based on the last time it was run", {
        incremental = true
      })
      opt("cleanup", "cleanup the temporary files that get created", {
        cleanup = true
      })
      opt("load", "load to duplicates into the database", {
        load = true
      })
      opt("f", "file", "A file that contains a list of species guids to detect duplication for", {
        v: String => speciesFile = Some(v)
      })
      opt("removeold", "Removes the duplicate information for records that are no longer duplicates", {
        removeObsoleteData = true
      })
      opt("od", "offlinedir", "The offline directory that contains the export files. Defaults to " + offlineDir, {
        v: String => offlineDir = v
      })
      opt("wd", "workingdir", "The  directory that contains the files produced during duplicate detection. Defaults to " + workingTmpDir, {
        v: String => workingTmpDir = v
      })
      intOpt("t", "threads", " The number of concurrent species duplications to perform. Defaults to " + threads, {
        v: Int => threads = v
      })
    }

    if (parser.parse(args)) {

      if (all) {
        //run duplicate detection for all taxa
        detectDuplicates(new File(workingTmpDir + "dd_all_species_guids"), threads, exist, cleanup, load, offlineDir)
      } else if (removeObsoleteData) {
        //remove obsolete duplicates against removed Taxon IDs (occurs when AFD re-mints their IDs)
        removeObsoleteDuplicates(speciesFile)
      } else if (guid.isDefined) {
        //just a single detection - ignore the thread settings etc...
        val dd = new DuplicationDetection
        val dataFilename = workingTmpDir + "dd_data_" + guid.get.replaceAll("[\\.:]", "_") + ".txt"
        val passedFilename = workingTmpDir + "passed" + guid.get.replaceAll("[\\.:]", "_") + ".txt"
        val dupFilename = workingTmpDir + "duplicates_" + guid.get.replaceAll("[\\.:]", "_") + ".txt"
        val inwdexFilename = workingTmpDir + "reindex_" + guid.get.replaceAll("[\\.:]", "_") + ".txt"
        val oldDup = workingTmpDir + "olddup_" + guid.get.replaceAll("[\\.:]", "_") + ".txt"

        if (load) {
          //load the results of a duplicate detection run into the persistence occurrence store
          dd.loadDuplicates(guid.get, threads, dupFilename, new FileWriter(oldDup))
          updateLastDuplicateTime
        } else {
          //run the duplicate detection
          dd.detect(dataFilename,
            new FileWriter(dupFilename),
            new FileWriter(passedFilename),
            guid.get,
            shouldDownloadRecords = !exist,
            cleanup = cleanup)
        }

        Config.persistenceManager.shutdown
        Config.indexDAO.shutdown
      } else if (speciesFile.isDefined) {
        detectDuplicates(new File(speciesFile.get), threads, exist, cleanup, load)
      } else {
        parser.showUsage
      }
    }
  }

  /**
    * Remove duplicates for taxa that are listed in the supplied file.
    * The file should contain a complete list of taxon GUIDs of interest. Any taxa that arent in this list
    * have been removed and hence and duplicates associated with removed taxa should be removed.
    *
    * @param filename
    */
  def removeObsoleteDuplicates(filename: Option[String]) {
    val olddupfilename = filename.getOrElse(workingTmpDir + "olddups.txt")
    new File(olddupfilename).foreachLine(line => {
      val parts = line.split("\t")
      val uuid = parts(1)
      Config.duplicateDAO.deleteObsoleteDuplicate(uuid)
    })
  }

  /**
    * Check to see if export files are available.
    *
    * @param offlineDir
    * @param threads
    * @return
    */
  def exportFilesAvailable(offlineDir: String, threads: Int): Boolean = {

    (0 until threads).foreach { threadId =>
      val file = new File(offlineDir + File.separator + threadId + File.separator + "species.out")
      if (!file.exists()) {
        logger.info("Log files available from previous export.")
        return false
      }
    }
    logger.info("Log files NOT available from previous export.")
    true
  }

  /**
    * Detect duplicates for all taxa
    *
    * @param file
    * @param threads
    * @param exist
    * @param cleanup
    * @param loadOnly
    * @param offlineDir
    */
  def detectDuplicates(file: File, threads: Int, exist: Boolean, cleanup: Boolean, loadOnly: Boolean, offlineDir: String = "") {

    logger.info(s"Starting duplicate detection with $threads threads")

    if (!loadOnly && !exportFilesAvailable(offlineDir, threads)) {
      logger.info(s"Exporting spatial data for duplicate detection....")
      val exporter = new ExportAllSpatialSpecies()
      exporter.export(false, threads, offlineDir)
      logger.info(s"Export spatial data for duplicate detection....finished")
    }

    val pool = Array.ofDim[Thread](threads)
    (0 until threads).foreach { threadId =>
      pool(threadId) = {
        val dir = workingTmpDir + threadId + File.separator
        FileUtils.forceMkdir(new File(dir))
        val dupfilename = dir + "duplicates.txt"
        val passedfilename = dir + "passed.txt"
        val indexfilename = dir + "reindex.txt"
        val olddupfilename = dir + "olddups.txt"

        val process = if (loadOnly) {
          logger.info("Starting loading thread with ID: " + threadId)
          new Thread() {
            override def run() {
              new DuplicationDetection().loadMultipleDuplicatesFromFile(
                dupfilename,
                passedfilename,
                threads,
                new FileWriter(new File(indexfilename)),
                new FileWriter(new File(olddupfilename)))
            }
          }
        } else {
          logger.info("Starting detection thread with ID: " + threadId)
          new Thread() {
            override def run() {
              //the writers should override the files because they will only ever have one instance...
              val sourceFileName = offlineDir + File.separator + threadId + File.separator + "species.out"
              logger.debug(s"Checking file $sourceFileName is available..")
              if (new File(sourceFileName).exists()) {
                new DuplicationDetection().detectMultipleDuplicatesFromFile(
                  sourceFileName,
                  new FileWriter(dupfilename),
                  new FileWriter(passedfilename),
                  threads)
              } else {
                logger.warn(s"Source file $sourceFileName not available.")
              }
            }
          }
        }

        process.start
        process
      }
    }

    pool.foreach(_.join)

    if (loadOnly) {
      //need to update the last duplication detection time
      updateLastDuplicateTime
      //need to merge all the obsolete duplicates into 1 file
      val baseFile = new File(workingTmpDir + "olddups.txt")
      for (i <- 0 to threads - 1) {
        val ifile = new File(workingTmpDir + i + File.separator + "olddups.txt")
        baseFile.append(ifile)
      }
    }
    Config.persistenceManager.shutdown
    Config.indexDAO.shutdown
  }

  /**
    * Store the time for the last duplicate run.
    */
  def updateLastDuplicateTime {
    val date = DateUtils.truncate(new java.util.Date(), java.util.Calendar.DAY_OF_MONTH)
    val cal = new java.util.GregorianCalendar()
    cal.setTime(date)
    cal.add(java.util.Calendar.HOUR, -24)
    Config.duplicateDAO.setLastDuplicationRun(cal.getTime())
  }
}

/**
  * An implementation of the duplicate detection methods devised by
  * Simon Bennett. The research and approach for this code is described in
  * this document:
  *
  * http://bit.ly/ALA-Duplicate-Notes
  *
  * TODO Use the "sensitive" coordinates for sensitive species
  */
class DuplicationDetection {

  import FileHelper._

  val logger = LoggerFactory.getLogger("DuplicateDetection")

  val baseDir = Config.tmpWorkDir
  val duplicatesFile = "duplicates.txt"
  val duplicatesToReindex = "duplicatesreindex.txt"
  val filePrefix = "dd_data.txt"

  val collectorNameLevenshteinDistanceThreshold = 3

  val fieldsToExport = Array(
    "row_key",
    "species_guid",
    "year",
    "month",
    "occurrence_date",
    "point-1",
    "point-0.1",
    "point-0.01",
    "point-0.001",
    "point-0.0001",
    "lat_long",
    "raw_taxon_name",
    "collectors",
    "duplicate_status",
    "duplicate_record",
    "record_number",
    "catalogue_number"
  )

  val speciesFilters = Array("lat_long:[* TO *]")

  // we have decided that a subspecies can be evaluated as part of the species level duplicates
  val subspeciesFilters = Array("lat_long:[* TO *]", "-species_guid:[* TO *]")

  val mapper = (new ObjectMapper).setSerializationInclusion(Include.NON_NULL)

  /**
    * Takes a dumpfile that was generated from the ExportAllRecordFacetFilter in multiple threads
    * Each file will be sorted by species guid enabling the code to read all the records for a
    * single taxon into memory.
    *
    * @param sourceFileName
    * @param threads
    */
  def detectMultipleDuplicatesFromFile(sourceFileName: String, duplicateWriter: FileWriter, passedWriter: FileWriter, threads: Int) {

    val reader = new CSVReader(new FileReader(sourceFileName), '\t', '~')

    var currentLine = reader.readNext //first line is header
    val buff = new ArrayBuffer[DuplicateRecordDetails]
    var counter = 0
    var currentLsid = ""
    while (currentLine != null) {

      if (currentLine.size >= fieldsToExport.length) {
        counter += 1
        if (counter % 10000 == 0) {
          logger.info("Loaded into memory : " + counter + " + records")
        }
        val rowKey = currentLine(0)
        val taxon_lsid = currentLine(1)

        if (currentLsid != taxon_lsid) {
          if (!buff.isEmpty) {
            logger.info("Read in " + counter + " records for GUID:" + currentLsid)
            //perform the duplication detection with the records that we have loaded.
            performDetection(buff.toList, duplicateWriter, passedWriter)
            buff.clear()
          }
          currentLsid = taxon_lsid
          counter = 1
          logger.info("Starting to detect duplicates for " + currentLsid)
        }
        val year = StringUtils.trimToNull(currentLine(3))
        val month = StringUtils.trimToNull(currentLine(4))

        val date: java.util.Date = try {
          DateUtils.parseDate(currentLine(5), "EEE MMM dd hh:mm:ss zzz yyyy")
        } catch {
          case _: Exception => null
        }
        val day = if (date != null) Integer.toString(date.getDate()) else null
        val rawName = StringUtils.trimToNull(currentLine(12))
        val collector = StringUtils.trimToNull(currentLine(13))
        val oldStatus = StringUtils.trimToNull(currentLine(14))
        val oldDuplicateOf = StringUtils.trimToNull(currentLine(15).replaceAll("\\[", "").replaceAll("\\]", ""))
        buff += new DuplicateRecordDetails(
          rowKey,
          rowKey,
          taxon_lsid,
          year,
          month,
          day,
          currentLine(6),
          currentLine(7),
          currentLine(8),
          currentLine(9),
          currentLine(10),
          currentLine(11),
          rawName,
          collector,
          oldStatus,
          oldDuplicateOf,
          currentLine(15),
          currentLine(16))
      } else {
        logger.warn("lsid " + currentLine(0) + " line " + counter + " has incorrect number of columns: "
          + currentLine.size + ", vs " + fieldsToExport.length)
      }
      currentLine = reader.readNext
    }
    logger.info("Read in " + counter + " records for GUID:" + currentLsid)
    //at this point we have all the records for a species that should be considered for duplication
    if (!buff.isEmpty) {
      performDetection(buff.toList, duplicateWriter, passedWriter)
    }
    duplicateWriter.close
  }

  /**
    * Perform the duplicate detection for the supplied list of records.
    *
    * This method groups the records by year, and runs detection on a separate thread for each year.
    *
    * @param records
    * @param duplicateWriter
    * @param passedWriter
    */
  def performDetection(records: List[DuplicateRecordDetails], duplicateWriter: FileWriter, passedWriter: FileWriter) {

    //group the records by year
    val yearGroups = records.groupBy {
      r => if (r.year != null) r.year else "UNKNOWN"
    }
    logger.debug("There are " + yearGroups.size + " year groups")

    //for each year, kick off a thread
    val threads = new ArrayBuffer[Thread]
    yearGroups.foreach {
      case (year, yearList) => {
        val t = new Thread(new YearGroupDetection(year, yearList, duplicateWriter, passedWriter))
        t.start()
        threads += t
      }
    }

    //now wait for each thread to finish
    threads.foreach(_.join)
    logger.debug("Finished processing each year")

    duplicateWriter.flush
  }

  /**
    * Loads the duplicates from a file that contains duplicates from multiple taxon concepts
    */
  def loadMultipleDuplicatesFromFile(dupFilename: String, passedFilename: String, threads: Int, reindexWriter: FileWriter, oldDuplicatesWriter: FileWriter) {

    var currentLsid = ""
    val queue = new ArrayBlockingQueue[String](100)
    val ids = new AtomicInteger(0)
    val buffer = new ArrayBuffer[String] // The buffer to store all the rowKeys that need to be reindexed
    val allDuplicates = new ArrayBuffer[String]
    val conceptPattern = """"taxonConceptLsid":"([A-Za-z0-9\-:\.]*)"""".r
//    var oldDuplicates: Set[String] = null
//    var oldDupMap: Map[String, String] = null
    val sentinel = UUID.randomUUID().toString() + System.currentTimeMillis()

    val pool: Array[StringConsumer] = Array.fill(threads) {
      val p = new StringConsumer(queue, ids.incrementAndGet(), sentinel,  {
        duplicate => {
          val duplicateRecordDetails = mapper.readValue[DuplicateRecordDetails](duplicate, classOf[DuplicateRecordDetails])
          persistDuplicate(duplicateRecordDetails, buffer, allDuplicates)
        }
      })
      p.start
      p
    }

    if (new File(dupFilename).exists()) {
      new File(dupFilename).foreachLine { line =>
        val lsidMatch = conceptPattern.findFirstMatchIn(line)
        if (lsidMatch.isDefined) {
          val strlsidMatch = lsidMatch.get.group(1)
          if (currentLsid != strlsidMatch) {
            //wait for the queue to be empty
            while (!queue.isEmpty) {
              Thread.sleep(200)
            }
//            if (oldDuplicates != null) {
//              buffer.foreach(v => reindexWriter.write(v + "\n"))
//              //revert the old duplicates that don't exist
//              revertNonDuplicateRecords(oldDuplicates, oldDupMap, allDuplicates.toSet, reindexWriter, oldDuplicatesWriter)
//              logger.info("REVERTING THE OLD duplicates for " + currentLsid)
//              buffer.reduceToSize(0)
//              allDuplicates.reduceToSize(0)
//              reindexWriter.flush
//              oldDuplicatesWriter.flush
//            }
//            //get new old duplicates
//            currentLsid = strlsidMatch
//            logger.info("STARTING to process the all the duplicates for " + currentLsid)
//            val olddds = getCurrentDuplicates(currentLsid)
//            oldDuplicates = olddds._1
//            oldDupMap = olddds._2
          }
          //add line to queue
          queue.put(line)
        }
      }
    } else {
      logger.error(s"$dupFilename does not exist - perhaps you need to run duplicate detection first...")
    }

    for (i <- 1 to threads) {
      queue.put(sentinel)
    }
    pool.foreach(_.join)
//
//    val olddds = getCurrentDuplicates(currentLsid)
//    oldDuplicates = olddds._1
//    oldDupMap = olddds._2
    buffer.foreach(v => reindexWriter.write(v + "\n"))
//    revertNonDuplicateRecords(oldDuplicates, oldDupMap, buffer.toSet, reindexWriter, oldDuplicatesWriter)
    reindexWriter.flush
    reindexWriter.close
//    oldDuplicatesWriter.flush
//    oldDuplicatesWriter.close
  }

  /**
   * Loads the duplicates from the lsid based on the tmp file being populated.
   * This is based on a single lsid being in the file
   */
  def loadDuplicates(lsid: String, threads: Int, dupFilename: String,  oldDupWriter: FileWriter) {
    //get a list of the current records that are considered duplicates
    val (oldDuplicates, oldDupMap) = getCurrentDuplicates(lsid)
    val queue = new ArrayBlockingQueue[String](100)
    val ids = new AtomicInteger(0)
    val buffer = new ArrayBuffer[String]
    val allDuplicates = new ArrayBuffer[String]
    val sentinel = UUID.randomUUID().toString() + System.currentTimeMillis()
    val pool: Array[StringConsumer] = Array.fill(threads) {
      val p = new StringConsumer(queue, ids.incrementAndGet(), sentinel, { duplicate =>
        val duplicateRecordDetails = mapper.readValue[DuplicateRecordDetails](duplicate, classOf[DuplicateRecordDetails])
         persistDuplicate(duplicateRecordDetails, buffer, allDuplicates)
      })
      p.start
      p
    }
    new File(dupFilename).foreachLine(line => queue.put(line))
    for (i <- 1 to threads) {
      queue.put(sentinel)
    }
    pool.foreach(_.join)
    revertNonDuplicateRecords(oldDuplicates, oldDupMap, allDuplicates.toSet, oldDupWriter)
    oldDupWriter.flush
    oldDupWriter.close
  }

  /**
   * Persists the specific duplicate details - allows duplicates to be loaded in a threaded manner
   */
  def persistDuplicate(primaryRecord: DuplicateRecordDetails, buffer: ArrayBuffer[String], allDuplicates: ArrayBuffer[String]) = {

    //turn the tool into the object
    logger.debug("HANDLING " + primaryRecord.rowKey)

    //get the duplicates that are not already part of the primary record
    allDuplicates.synchronized {
      allDuplicates += primaryRecord.rowKey
      primaryRecord.duplicates.foreach(d => allDuplicates += d.rowKey)
    }

    val newduplicates = primaryRecord.duplicates.filter { _.oldDuplicateOf != primaryRecord.uuid }
    val uuidList = primaryRecord.duplicates.map(r => r.uuid)

    try {

      if (!newduplicates.isEmpty || primaryRecord.oldDuplicateOf == null || primaryRecord.duplicates.size != primaryRecord.oldDuplicateOf.split(",").size) {

        buffer.synchronized {
          buffer += primaryRecord.rowKey
        }

        //save duplicate details
        Config.duplicateDAO.saveDuplicate(primaryRecord)

        Config.persistenceManager.put(
          primaryRecord.rowKey,
          "occ",
          Map(
            "associatedOccurrences" + Config.persistenceManager.fieldDelimiter + "p" -> uuidList.mkString("|"),
            "duplicationStatus" + Config.persistenceManager.fieldDelimiter + "p" -> "R"
          ),
          true,
          false
        )

        //store details for new duplicates
        newduplicates.foreach { r =>
          val types = if (r.dupTypes != null) {
            r.dupTypes.toList.map(t => t.getId.toString).toArray[String]
          } else {
            Array[String]()
          }

          saveToOccurrenceRecord(primaryRecord, r, types)

          buffer.synchronized {
            buffer += r.rowKey
          }
        }
      }
    } catch {
      case e: Exception => e.printStackTrace();
    }
  }

  /**
    * Persists the details of duplication against the record (for indexing purposes).
    *
    * @param primaryRecord
    * @param duplicateDetails
    * @param duplicateTypes
    */
  private def saveToOccurrenceRecord(primaryRecord: DuplicateRecordDetails, duplicateDetails: DuplicateRecordDetails, duplicateTypes: Array[String]) = {
    //store the associated occurrences on the record
    Config.persistenceManager.put(
      duplicateDetails.rowKey,
      "occ",
      Map(
        "associatedOccurrences" + Config.persistenceManager.fieldDelimiter + "p" -> primaryRecord.uuid,
        "duplicationStatus" + Config.persistenceManager.fieldDelimiter + "p" -> "D",
        "duplicationType" + Config.persistenceManager.fieldDelimiter + "p" -> mapper.writeValueAsString(duplicateTypes)
      ),
      true,
      false
    )

    //add a system message for the record - a duplication does not change the kosher fields
    // and should always be displayed thus don't "checkExisting"
    Config.occurrenceDAO.addSystemAssertion(
      duplicateDetails.rowKey,
      QualityAssertion(
        AssertionCodes.INFERRED_DUPLICATE_RECORD,
        "Record has been inferred as closely related to  " + primaryRecord.uuid
      ),
      false
    )
  }

  def downloadRecords(sourceFileName: String, lsid: String, field: String) {
    val file = new File(sourceFileName)
    FileUtils.forceMkdir(file.getParentFile)
    val fileWriter = new FileWriter(file)
    logger.info("Starting to download the occurrences for " + lsid)
    ExportByFacetQuery.downloadSingleTaxonByStream(
      null,
      lsid,
      fieldsToExport,
      field,
      if (field == "species_guid") speciesFilters else subspeciesFilters,
      Array("row_key"),
      fileWriter,
      None,
      Some(Array("duplicate_record")))
    fileWriter.close
  }

  /**
    * Performs the tool detection - each year of records is processed on a separate thread.
    * WARNING as of 2013-08-30 This method should only be used to detect the duplicates for a single species.
    *
    * @param sourceFileName
    * @param duplicateWriter
    * @param passedWriter
    * @param lsid
    * @param shouldDownloadRecords
    * @param field
    * @param cleanup
    */
  def detect(sourceFileName: String, duplicateWriter: FileWriter, passedWriter: FileWriter, lsid: String,
             shouldDownloadRecords: Boolean = false, field: String = "species_guid", cleanup: Boolean = false) {

    logger.info("Starting to detect duplicates for " + lsid)

    if (shouldDownloadRecords) {
      downloadRecords(sourceFileName, lsid, field)
    }

    //open the tmp file that contains the information about the lsid
    val reader = new CSVReader(new FileReader(sourceFileName), '\t', '`', '~')
    var currentLine = reader.readNext //first line is data, not header
    val buff = new ArrayBuffer[DuplicateRecordDetails]
    var counter = 0

    while (currentLine != null) {
      if (currentLine.size >= 16) {
        counter += 1
        if (counter % 10000 == 0) {
          logger.debug("Loaded into memory : " + counter + " + records")
        }
        val rowKey = currentLine(0)
        val taxon_lsid = currentLine(1)
        val year = StringUtils.trimToNull(currentLine(2))
        val month = StringUtils.trimToNull(currentLine(3))

        val date: java.util.Date = try {
          DateUtils.parseDate(currentLine(4), "EEE MMM dd hh:mm:ss zzz yyyy")
        } catch {
          case _: Exception => null
        }
        val day = if (date != null) Integer.toString(date.getDate()) else null
        val rawName = StringUtils.trimToNull(currentLine(11))
        val collector = StringUtils.trimToNull(currentLine(12))
        val oldStatus = StringUtils.trimToNull(currentLine(13))
        val oldDuplicateOf = StringUtils.trimToNull(currentLine(14).replaceAll("\\[", "").replaceAll("\\]", ""))
        buff += new DuplicateRecordDetails(rowKey, rowKey, taxon_lsid, year, month, day, currentLine(5), currentLine(6),
          currentLine(7), currentLine(8), currentLine(9), currentLine(10), rawName, collector, oldStatus, oldDuplicateOf,
          currentLine(15), currentLine(16))
      } else {
        logger.warn("lsid " + lsid + " line " + counter + " has incorrect column number: " + currentLine.size)
      }
      currentLine = reader.readNext
    }

    logger.info("Read in " + counter + " records for " + lsid)
    //at this point we have all the records for a species that should be considered for duplication
    val allRecords = buff.toList
    val yearGroups = allRecords.groupBy {
      r => if (r.year != null) r.year else "UNKNOWN"
    }
    logger.debug("There are " + yearGroups.size + " year groups")
    val threads = new ArrayBuffer[Thread]
    yearGroups.foreach {
      case (year, yearList) => {
        val t = new Thread(new YearGroupDetection(year, yearList, duplicateWriter, passedWriter))
        t.start()
        threads += t
      }
    }

    //now wait for each thread to finish
    threads.foreach(_.join)
    logger.debug("Finished processing each year")

    duplicateWriter.flush
    duplicateWriter.close
  }

  /**
   * Changes the stored values for the "old" duplicates that are no longer considered duplicates
   */
  def revertNonDuplicateRecords(oldDuplicates: Set[String], oldDupMap: Map[String, String], currentDuplicates: Set[String], oldWriter: FileWriter) {
    val nonDuplicates = oldDuplicates -- currentDuplicates
    nonDuplicates.foreach { nd =>
      logger.warn(nd + " is no longer a duplicate")
      //remove the duplication columns
      Config.persistenceManager.deleteColumns(nd, "occ", "associatedOccurrences" + Config.persistenceManager.fieldDelimiter + "p", "duplicationStatus" + Config.persistenceManager.fieldDelimiter + "p", "duplicationType" + Config.persistenceManager.fieldDelimiter + "p")
      //now remove the system assertion if necessary
      Config.occurrenceDAO.removeSystemAssertion(nd, AssertionCodes.INFERRED_DUPLICATE_RECORD)
      oldWriter.write(nd + "\t" + oldDupMap(nd) + "\n")
    }
  }

  /**
    * Gets a list of current duplicates so that records no longer considered a tool can be reset
    */
  def getCurrentDuplicates(lsid: String): (Set[String], Map[String, String]) = {
//    val startKey = lsid + "|"
//    val endKey = lsid + "|~"
//
//    val buf = new ArrayBuffer[String]
//    val uuidMap = new scala.collection.mutable.HashMap[String, String]()
//
//    Config.persistenceManager.pageOverAll("duplicates", (guid, map) => {
//      logger.debug("Getting old duplicates for " + guid)
//      map.values.foreach(v => {
//        //turn it into a DuplicateRecordDetails
//        val rd = mapper.readValue[DuplicateRecordDetails](v, classOf[DuplicateRecordDetails])
//        buf += rd.rowKey
//        uuidMap += rd.rowKey -> rd.uuid
//        rd.duplicates.toList.foreach(d => {
//          buf += d.rowKey
//          uuidMap += d.rowKey -> d.uuid
//        })
//      })
//      true
//    }, startKey, endKey, 100)
//
//    (buf.toSet, uuidMap.toMap[String, String])
    (Set(), Map())
  }

  /**
    * Duplicate detection for the supplied records which are all for the same year.
    * Each year is handled separately so they can be processed in a threaded manner
    */
  class YearGroupDetection(year: String, records: List[DuplicateRecordDetails], duplicateWriter: FileWriter,
                           passedWriter: FileWriter) extends Runnable {

    val latLonPattern = """(\-?\d+(?:\.\d+)?),\s*(\-?\d+(?:\.\d+)?)""".r
    val alphaNumericPattern = "[^\\p{L}\\p{N}]".r
    val unknownPatternString = "(null|UNKNOWN OR ANONYMOUS)"
    val mapper = new ObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.setSerializationInclusion(Include.NON_NULL)

    override def run() = {

      logger.debug("Starting duplication detection for the year:  " + year)
      val monthGroups = records.groupBy(r => if (r.month != null) r.month else "UNKNOWN")
      val buffGroups = new ArrayBuffer[DuplicateRecordDetails]
      monthGroups.foreach {
        case (month, monthList) => {
          //if there is more than 1 record group by days
          if (monthList.size > 1) {
            val dayGroups = monthList.groupBy(r => if (r.day != null) r.day else "UNKNOWN")
            dayGroups.foreach {
              case (day, dayList) => {
                if (dayList.size > 1) {
                  //need to check for duplicates
                  buffGroups ++= checkDuplicates(dayList)
                } else {
                  buffGroups += dayList.head
                }
              }
            }
          } else {
            buffGroups += monthList.head
          }
        }
      }

      logger.debug("Number of distinct records for year " + year + " is " + buffGroups.size)

      buffGroups.foreach { record =>
        if (record.duplicates != null && !record.duplicates.isEmpty) {
          val (primaryRecord, duplicates) = markRecordsAsDuplicatesAndSetTypes(record)
          val stringValue = mapper.writeValueAsString(primaryRecord)
          //write the tool to file to be handled at a later time
          duplicateWriter.synchronized {
            duplicateWriter.write(stringValue + "\n")
          }
        } else if ((record.duplicates == null || record.duplicates.isEmpty) && StringUtils.isBlank(record.duplicateOf)) {
          //this record has passed its tool detection
          passedWriter.synchronized {
            passedWriter.write(record.getRowKey + "\n")
          }
        }
        logger.debug("RECORD: " + record.rowKey + " has " + record.duplicates.size + " duplicates")
      }

      duplicateWriter.synchronized {
        duplicateWriter.flush()
      }

      passedWriter.synchronized {
        passedWriter.flush()
      }
    }

    def setDateTypes(r: DuplicateRecordDetails, hasYear: Boolean, hasMonth: Boolean, hasDay: Boolean) = {
      if (hasYear && hasMonth && !hasDay) {
        r.dupTypes = r.dupTypes ++ Array(DuplicationTypes.MISSING_DAY)
      } else if (hasYear && !hasMonth) {
        r.dupTypes = r.dupTypes ++ Array(DuplicationTypes.MISSING_MONTH)
      } else if (!hasYear) {
        r.dupTypes = r.dupTypes ++ Array(DuplicationTypes.MISSING_YEAR)
      }
      r
    }

    def markRecordsAsDuplicatesAndSetTypes(record: DuplicateRecordDetails): (DuplicateRecordDetails, Seq[DuplicateRecordDetails]) = {
      //find the "representative" record for the tool
      var highestPrecision = determinePrecision(record.latLong)
      record.precision = highestPrecision
      var representativeRecord = record

      val duplicates = new ArrayBuffer[DuplicateRecordDetails]()
      duplicates.appendAll(record.duplicates)

      //find out whether or not record has date components
      val hasYear = StringUtils.isNotEmpty(record.year)
      val hasMonth = StringUtils.isNotEmpty(record.month)
      val hasDay = StringUtils.isNotEmpty(record.day)

      setDateTypes(record, hasYear, hasMonth, hasDay)

      duplicates.foreach(r => {
        setDateTypes(r, hasYear, hasMonth, hasDay)
        r.precision = determinePrecision(r.latLong)
        if (r.precision > highestPrecision) {
          highestPrecision = r.precision
          representativeRecord = r
        }
      })

      representativeRecord.status = "R"

      if (representativeRecord != record) {
        record.duplicates = Array()
        duplicates += record
        duplicates -= representativeRecord
        representativeRecord.duplicates = duplicates.toArray
        //set the duplication types of the old rep record
        record.dupTypes = representativeRecord.dupTypes
      }

      //set the duplication type based data resource uid
      duplicates.foreach { d =>
        d.status = if (d.druid == representativeRecord.druid) {
          "D1"
        } else {
          "D2"
        }
        if (d.precision == representativeRecord.precision) {
          d.dupTypes = d.dupTypes ++ Array(DuplicationTypes.EXACT_COORD)
        } else {
          d.dupTypes = d.dupTypes ++ Array(DuplicationTypes.DIFFERENT_PRECISION)
        }
      }

      (representativeRecord, duplicates)
    }

    //reports the maximum number of decimal places that the lat/long are reported to
    def determinePrecision(latLong: String): Int = {
      try {
        val latLonPattern(lat, long) = latLong
        val latp = if (lat.contains(".")) lat.split("\\.")(1).length else 0
        val lonp = if (long.contains(".")) long.split("\\.")(1).length else 0
        if (latp > lonp) {
          latp
        } else {
          lonp
        }
      } catch {
        case e: Exception => logger.error("ISSUE WITH " + latLong, e); 0
      }
    }

    /**
      * Check for duplicates for the supplied list. Only checks records that are not already considered to a duplicate
      * of another record.
      *
      * @param recordGroup the records to test
      * @return duplicate record details list for records considered to have duplicates
      */
    def checkDuplicates(recordGroup: List[DuplicateRecordDetails]): List[DuplicateRecordDetails] = {
      recordGroup.foreach(record => {
        if (record.duplicateOf == null) {
          //this record needs to be considered for duplication
          findDuplicates(record, recordGroup)
        }
      })
      recordGroup.filter(_.duplicateOf == null)
    }

    /**
      * Find duplicates of the supplied record from within the supplied record group.
      *
      * @param record      the record to test with
      * @param recordGroup the record set to find duplicates within
      */
    def findDuplicates(record: DuplicateRecordDetails, recordGroup: List[DuplicateRecordDetails]) {

      val points = Array(
        record.point1,
        record.point0_1,
        record.point0_01,
        record.point0_001,
        record.point0_0001,
        record.latLong)

      val dupBuffer = new ArrayBuffer[DuplicateRecordDetails]()
      dupBuffer.appendAll(record.duplicates)

      recordGroup.foreach { otherRecord =>
        if (otherRecord.duplicateOf == null && record.rowKey != otherRecord.rowKey) {

          val otherpoints = Array(
            otherRecord.point1,
            otherRecord.point0_1,
            otherRecord.point0_01,
            otherRecord.point0_001,
            otherRecord.point0_0001,
            otherRecord.latLong)

          //if the spatial coordinates are considered the same (regardless of mismatches in precision)
          // and the normalised collectory name are the same, then we have a duplicate
          if (isSpatialDuplicate(points, otherpoints)) {
            // the below statement ensures all duplicate check functions are called
            val isCollectorDup: Boolean = isCollectorDuplicate(record, otherRecord);
            val isRecordNumberDup: Boolean = isRecordNumberDuplicate(record, otherRecord);
            val isCatalogueNumberDup: Boolean = isCatalogueNumberDuplicate(record, otherRecord);
            if (isCollectorDup || isRecordNumberDup || isCatalogueNumberDup) {
              otherRecord.duplicateOf = record.rowKey
              dupBuffer.append(otherRecord)
            }
          }
        }
      }

      record.duplicates = dupBuffer.toArray
    }

    /**
      * Check if two records has the same catalogue number
      *
      * @param r1
      * @param r2
      * @return
      */
    def isCatalogueNumberDuplicate(r1: DuplicateRecordDetails, r2: DuplicateRecordDetails): Boolean = {
      if (r1.catalogueNumber != null && r2.catalogueNumber != null) {
        if (isEmptyUnknown(r1.catalogueNumber) || isEmptyUnknown(r2.catalogueNumber)) {
          false
        } else if (StringUtils.trim(r1.catalogueNumber.toLowerCase()) == StringUtils.trim(r2.catalogueNumber.toLowerCase())) {
          r2.dupTypes = r2.dupTypes ++ Array(DuplicationTypes.EXACT_CATALOGUE_NUMBER)
          true
        } else {
          false
        }
      } else {
        false
      }
    }

    /**
      * Check if two records has the same record number
      *
      * @param r1
      * @param r2
      * @return
      */
    def isRecordNumberDuplicate(r1: DuplicateRecordDetails, r2: DuplicateRecordDetails): Boolean = {
      if (r1.recordNumber != null && r2.recordNumber != null) {
        if (isEmptyUnknown(r1.recordNumber) || isEmptyUnknown(r2.recordNumber)) {
          false
        } else if (StringUtils.trim(r1.recordNumber.toLowerCase()) == StringUtils.trim(r2.recordNumber.toLowerCase())) {
          r2.dupTypes = r2.dupTypes ++ Array(DuplicationTypes.EXACT_FIELD_NUMBER)
          true
        } else {
          false
        }
      } else {
        false
      }
    }

    /**
      * Is an empty or unknown collector string.
      *
      * @param in
      * @return
      */
    def isEmptyUnknown(in: String): Boolean =
      StringUtils.isEmpty(in) || in.matches(unknownPatternString)

    /**
      * Compare the collector names for the supplied records. This will return true if:
      *
      * <ul>
      * <li>If we have a unknown collector for either record </li>
      * <li>If we have a exact match </li>
      * <li>If we have a fuzzy match with 3 or less differences </li>
      * </ul>
      *
      * @param r1
      * @param r2
      * @return true if the collector names are the same.
      */
    def isCollectorDuplicate(r1: DuplicateRecordDetails, r2: DuplicateRecordDetails): Boolean = {

      //if one of the collectors haven't been supplied assume that they are the same.
      if (isEmptyUnknown(r1.collector) || isEmptyUnknown(r2.collector)) {
        if (isEmptyUnknown(r2.collector))
          r2.dupTypes = r2.dupTypes ++ Array(DuplicationTypes.MISSING_COLLECTOR)
        true
      } else {
        val (col1, col2) = prepareCollectorsForLevenshtein(r1.collector, r2.collector)
        val distance = StringUtils.getLevenshteinDistance(col1, col2)
        //allow 3 differences in the collector name
        if (distance <= collectorNameLevenshteinDistanceThreshold) {
          if (distance > 0) {
            //we have a fuzzy match
            r2.dupTypes = r2.dupTypes ++ Array(DuplicationTypes.FUZZY_COLLECTOR)
          } else {
            //we have an exact match
            r2.dupTypes = r2.dupTypes ++ Array(DuplicationTypes.EXACT_COLLECTOR)
          }
          true
        } else {
          false
        }
      }
    }

    /**
      * Returns strings of the same length choosing the length of the shorter string.
      *
      * @param c1
      * @param c2
      * @return
      */
    def prepareCollectorsForLevenshtein(c1: String, c2: String): (String, String) = {
      //remove all the non alphanumeric characters
      val c11 = alphaNumericPattern.replaceAllIn(c1, "")
      val c21 = alphaNumericPattern.replaceAllIn(c2, "")
      val length = if (c11.size > c21.size) c21.size else c11.size
      (c11.substring(0, length), c21.substring(0, length))
    }

    /**
      * Returns true of the supplied points.
      *
      * @param pointsA lat,lng values for record 1 at different precisions
      * @param pointsB lat,lng values for record 1 at different precisions
      * @return true if the values are considered the same
      */
    def isSpatialDuplicate(pointsA: Array[String], pointsB: Array[String]): Boolean = {

      if (pointsB.length != pointsA.length) {
        throw new Exception("Points supplied with a differing number of precisions")
      }

      for (i <- 0 until pointsA.length) {

        if (pointsA(i) != pointsB(i)) {
          //check to see if the precision is different
          if (i > 0) {
            //one of the current points has the same coordinates as the previous precision
            if (pointsA(i) == pointsA(i - 1) || pointsB(i) == pointsB(i - 1)) {
              if (i < 5) {
                //indicates that we have a precision difference
                if (pointsA(i) == pointsA(i + 1) || pointsB(i) == pointsA(i + 1))
                  return true
              } else {
                return true
              }
            }
            //now check if we have a rounding error by look at the subsequent coordinates...
            return false
          } else {
            //at the largest granularity the coordinates are different
            return false
          }
        }
      }
      true
    }
  }
}
