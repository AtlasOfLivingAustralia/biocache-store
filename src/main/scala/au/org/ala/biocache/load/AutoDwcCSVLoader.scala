package au.org.ala.biocache.load

import java.io._
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.io.{FilenameUtils,FileUtils}
import org.apache.commons.compress.utils.IOUtils
import au.org.ala.biocache.Config
import au.com.bytecode.opencsv.CSVReader
import scala.Some
import scala.Console
import collection.mutable.ArrayBuffer
import au.org.ala.biocache.util.{OptionParser, FileHelper}
import au.org.ala.biocache.vocab.DwC

/**
 * Loads a data resource that provides reloads and incremental updates.
 * 
 * Files of this form are produced according to the format that Bryn Kingsford developed.
 *
 * The archive will have:
 * <ul>
 * <li> data files - contains the information that need to be inserted or updated </li>
 * <li> id files - contain the identifying fields for all the current records for the data resource.
 * Records that don't have identifiers in the field will need to be removed from cassandra </li>
 * </ul>
 *  
 * The id files will be treated as if they are data files. They will contain all the identifiers
 * (collection code, institution code and catalogue numbers) for the occurrences. Thus when they are loaded
 * the last modified date will be updated and the record will be considered current.
 */
object AutoDwcCSVLoader {

  def main(args: Array[String]) {

    var dataResourceUid = ""
    var localFilePath: Option[String] = None
    var processIds = false

    val parser = new OptionParser("import darwin core headed CSV") {
      arg("<data-resource-uid>", "the data resource to import", {
        v: String => dataResourceUid = v
      })
      opt("l", "local", "skip the download and use local file", {
        v: String => localFilePath = Some(v)
      })
      opt("ids", "load the ids files", {
        processIds = true
      })
    }

    if (parser.parse(args)) {
      val l = new AutoDwcCSVLoader
      try {
        localFilePath match {
          case None => l.load(dataResourceUid, processIds)
          case Some(v) => l.loadLocalFile(dataResourceUid, v, processIds)
        }
        //update the collectory information
        l.updateLastChecked(dataResourceUid)
      } catch {
        case e: Exception => e.printStackTrace
      } finally {
//        l.pm.shutdown
        Console.flush()
        Console.err.flush()
        sys.exit(0)
      }
    }
  }
}

class AutoDwcCSVLoader extends DataLoader {

  import FileHelper._

  val loadPattern = """([\x00-\x7F\s]*dwc[\x00-\x7F\s]*.csv[\x00-\x7F\s]*)""".r
  logger.info("Load pattern in use: " + loadPattern.toString())

  def load(dataResourceUid: String, includeIds: Boolean = true, forceLoad: Boolean = false) {

    retrieveConnectionParameters(dataResourceUid) match {

      case None => throw new Exception ("Unable to retrieve configuration for dataResourceUid " + dataResourceUid)

      case Some(dataResourceConfig) =>

        val strip = dataResourceConfig.connectionParams.getOrElse("strip", false).asInstanceOf[Boolean]
        //clean out the dr load directory before downloading the new file.
        emptyTempFileStore(dataResourceUid)
        //remove the old file
        deleteOldRowKeys(dataResourceUid)
        var loaded = false
        var maxLastModifiedDate: java.util.Date = null
        //the supplied url should be an sftp string to the directory that contains the dumps
        dataResourceConfig.urls.foreach(url => {
          if (url.startsWith("sftp")) {
            val fileDetails = sftpLatestArchive(url, dataResourceUid, if (forceLoad) None else dataResourceConfig.dateLastChecked)
            if (fileDetails.isDefined) {
              val (filePath, date) = fileDetails.get
              if (maxLastModifiedDate == null || date.after(maxLastModifiedDate)) {
                maxLastModifiedDate = date
              }
              loadAutoFile(new File(filePath), dataResourceUid, dataResourceConfig, includeIds, strip)
              loaded = true
            }
          } else {
            logger.error("Unable to process " + url + " with the auto loader")
          }
        })
        //now update the last checked and if necessary data currency dates
        updateLastChecked(dataResourceUid, if (loaded) Some(maxLastModifiedDate) else None)
        if (!loaded) {
          setNotLoadedForOtherPhases(dataResourceUid)
        }
    }
  }

  def loadLocalFile(dataResourceUid: String, filePath: String, includeIds: Boolean) {
    retrieveConnectionParameters(dataResourceUid) match {
      case None => throw new Exception ("Unable to retrieve configuration for dataResourceUid " + dataResourceUid)
      case Some(dataResourceConfig) =>
        val strip = dataResourceConfig.connectionParams.getOrElse("strip", false).asInstanceOf[Boolean]
        //remove the old file
        deleteOldRowKeys(dataResourceUid)
        loadAutoFile(new File(filePath), dataResourceUid, dataResourceConfig, includeIds, strip)
    }
  }

  def loadAutoFile(file: File, dataResourceUid: String, dataResourceConfig:DataResourceConfig, includeIds: Boolean, stripSpaces: Boolean) {
    //From the file extract the files to load
    val baseDir = file.getParent
    val csvLoader = new DwcCSVLoader
    if (file.getName.endsWith(".tar.gz") || file.getName.endsWith(".tgz") || file.getName().endsWith(".zip")) {
      //set up the lists of data and id files
      val dataFiles = new scala.collection.mutable.ListBuffer[File]
      //val idFiles = new scala.collection.mutable.ListBuffer[File]

      //now find a list of files that obey the data load file
      val tarInputStream = {
        if (file.getName.endsWith("tar.gz") || file.getName.endsWith(".tgz")) {
          //gunzip the file
          val unzipedFile = file.extractGzip
          new ArchiveStreamFactory().createArchiveInputStream("tar", new FileInputStream(unzipedFile))
        } else {
          new ArchiveStreamFactory().createArchiveInputStream("zip", new FileInputStream(file))
        }
      }
      var entry = tarInputStream.getNextEntry
      var hasImages = false
      while (entry != null) {
        val name: String = FilenameUtils.getName(entry.getName)
        logger.debug("File from archive name: " + name)
        name match {
          case loadPattern(filename) => dataFiles += extractTarFile(tarInputStream, baseDir, entry.getName)
          case it if Config.mediaStore.endsWithOneOf(Config.mediaStore.imageExtension, it) => {
            //supports images supplied in the archive. Relies on the images paths being supplied in associatedMedia
            // as a path relative to the directory of the CSV file
            extractTarFile(tarInputStream, baseDir, entry.getName)
            hasImages = true
          }
          case _ => //do nothing with the file
        }
        entry = tarInputStream.getNextEntry
      }

      logger.info(dataFiles.toString())

      //val dwcfiles = dataFiles.fil
      var validRowKeys = List[String]()
      //Now take the load files and use the DwcCSVLoader to load them
      dataFiles.foreach(dfile => {
        if ((!dfile.getName().contains("dwc-id") && !dfile.getName().contains("dwcid"))) {
          val storeKeys = !dfile.getName().contains("dwc-id") && !dfile.getName().contains("dwcid")
          logger.info("Loading " + dfile.getName() + " storing the keys for reprocessing: " + storeKeys)
          if (dfile.getName.endsWith("gz")) {
            csvLoader.loadFile(dfile.extractGzip, dataResourceUid, dataResourceConfig.uniqueTerms, dataResourceConfig.connectionParams, stripSpaces, logRowKeys = storeKeys)
          } else {
            csvLoader.loadFile(dfile, dataResourceUid, dataResourceConfig.uniqueTerms, dataResourceConfig.connectionParams, stripSpaces, logRowKeys = storeKeys)
          }
        } else {
          //load the id's into a list of valid rowKeys
          val rowKeyFile = if (dfile.getName.endsWith(".gz")) {
            dfile.extractGzip
          } else {
            dfile
          }

          validRowKeys ++= extractValidRowKeys(rowKeyFile, dataResourceUid, dataResourceConfig.uniqueTerms, dataResourceConfig.connectionParams, stripSpaces)
          logger.info("Number of validRowKeys: " + validRowKeys.size)
        }
      })

      //Create the list of distinct row keys that are no longer in the source system
      //This is achieved by extracting all the current row keys from the index and removing the values that we loaded from the current id files.
      val listbuf = new ArrayBuffer[String]()
      logger.info("Retrieving the current rowKeys for the dataResource...")
      Config.indexDAO.streamIndex(map => {
        listbuf += map.get("row_key").toString
        true
      }, Array("row_key"), "data_resource_uid:" + dataResourceUid, Array(), Array(), None)
      logger.info("Number of currentRowKeys: " + listbuf.size)
      val deleted = listbuf.toSet &~ validRowKeys.toSet
      logger.info("Number to delete deleted " + deleted.size)
      if (!deleted.isEmpty) {
        val writer = getDeletedFileWriter(dataResourceUid)
        deleted.foreach(line => writer.write(line + "\n"))
        writer.flush()
        writer.close()
      }
    }
  }

  /**
   * Takes a DWC CSV file and outputs a list of all the unique identifiers for the records
   * @param file
   * @param dataResourceUid
   * @param uniqueTerms
   * @param params
   * @param stripSpaces
   * @return
   */
  def extractValidRowKeys(file: File, dataResourceUid: String, uniqueTerms: Seq[String], params: Map[String, String], stripSpaces: Boolean): List[String] = {
    logger.info("Extracting the valid row keys from " + file.getAbsolutePath)
    val quotechar = params.getOrElse("csv_text_enclosure", "\"").head
    val separator = {
      val separatorString = params.getOrElse("csv_delimiter", ",")
      if (separatorString == "\\t") '\t'
      else separatorString.toCharArray.head
    }
    val escape = params.getOrElse("csv_escape_char", "|").head
    val reader = new CSVReader(new InputStreamReader(new org.apache.commons.io.input.BOMInputStream(new FileInputStream(file))), separator, quotechar, escape)

    logger.info("Using CSV reader with the following settings quotes: " + quotechar + " separator: " + separator + " escape: " + escape)
    //match the column headers to dwc terms
    val dwcTermHeaders = {
      val headerLine = reader.readNext
      if (headerLine != null) {
        val columnHeaders = headerLine.map(t => t.replace(" ", "").trim).toList
        DwC.retrieveCanonicals(columnHeaders)
      } else {
        null
      }
    }

    val listbuf = new ArrayBuffer[String]()

    if (dwcTermHeaders == null) {
      logger.warn("No content in file.")
      return listbuf.toList
    }

    var currentLine = reader.readNext

    if (dwcTermHeaders == null) {
      logger.warn("No content in file.")
      return listbuf.toList
    }

    logger.info("Unique terms: " + uniqueTerms)
    logger.info("Column headers: " + dwcTermHeaders)

    val validConfig = uniqueTerms.forall(t => dwcTermHeaders.contains(t))
    if (!validConfig) {
      throw new RuntimeException("Bad configuration for file: " + file.getName + " for resource: " +
        dataResourceUid + ". CSV file is missing unique terms.")
    }
    while (currentLine != null) {

      val columns = currentLine.toList
      if (columns.length >= dwcTermHeaders.size - 1) {
        val map = (dwcTermHeaders zip columns).toMap[String, String].filter({
          case (key, value) => {
            if (value != null) {
              val upperCased = value.trim.toUpperCase
              upperCased != "NULL" && upperCased != "N/A" && upperCased != "\\N" && upperCased != ""
            } else {
              false
            }
          }
        })

        val uniqueTermsValues = uniqueTerms.map(t => map.getOrElse(t, ""))
        listbuf += createUniqueID(dataResourceUid, uniqueTermsValues, stripSpaces)
        currentLine = reader.readNext()
      }
    }
    listbuf.toList
  }

  def extractTarFile(io: InputStream, baseDir: String, filename: String): File = {
    //construct the file
    logger.info("Extracting " + filename + " to " + baseDir)
    val file = new File(baseDir + System.getProperty("file.separator") + filename)
    FileUtils.forceMkdir(file.getParentFile)
    val fos = new FileOutputStream(file)
    IOUtils.copy(io, fos)
    fos.flush
    fos.close
    file
  }
}