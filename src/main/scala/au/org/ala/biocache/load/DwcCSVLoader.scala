package au.org.ala.biocache.load

import java.io.{File, FileInputStream, InputStreamReader}

import au.com.bytecode.opencsv.CSVReader
import au.org.ala.biocache._
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.model.Versions
import au.org.ala.biocache.parser.DateParser
import au.org.ala.biocache.util.OptionParser
import au.org.ala.biocache.vocab.DwC
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * Companion object for the DwcCSVLoader class
 */
object DwcCSVLoader extends Tool {

  def cmd = "load-local-csv"
  def desc = "Load local CSV file. Not for production use."
    
  def main(args:Array[String]){

    var dataResourceUid = ""
    var localFilePath:Option[String] = None
    var updateLastChecked = false
    var bypassConnParamLookup = false
    var testFile = false
    var logRowKeys = true
    val logger = LoggerFactory.getLogger("DwcCSVLoader")
    var uniqueTerms = Array("occurrenceID")

    val parser = new OptionParser(help) {
      arg("data-resource-uid", "the data resource to import", {v: String => dataResourceUid = v})
      opt("l", "local", "skip the download and use local file", {v:String => localFilePath = Some(v) } )
      opt("u", "updateLastChecked", "update registry with last loaded date. defaults to " + updateLastChecked, {
        updateLastChecked = true
      })
      opt("b", "bypassConnParamLookup", "bypass connection param lookup. defaults to " + bypassConnParamLookup, {
        bypassConnParamLookup = true
      })
      opt("test", "test the file only do not load", { testFile = true })

      opt("ut", "uniqueTerms", "Unique terms to use to make an ID. Comma separated DwC", {
        v: String => uniqueTerms = v.split(",").map { _.trim }
      })
    }

    if(parser.parse(args)){
      val l = new DwcCSVLoader
      l.deleteOldRowKeys(dataResourceUid)
      try {
        if (bypassConnParamLookup && !localFilePath.isEmpty){
          l.loadFile(
            new File(localFilePath.get),
            dataResourceUid,
            uniqueTerms,
            Map(),
            false,
            logRowKeys,
            testFile
          )
        } else {
          localFilePath match {
            case None => l.load(dataResourceUid, logRowKeys, testFile)
            case Some(filePath) => l.loadLocalFile(dataResourceUid, filePath, logRowKeys, testFile)
          }
          //initialise the delete/update the collectory information
          if (updateLastChecked){
            l.updateLastChecked(dataResourceUid)
          }
        }
      } catch {
        case e:Exception => logger.error(e.getMessage, e)
      }
    }
  }
}

/**
 * Class of loading a CSV with darwin core term headers.
 */
class DwcCSVLoader extends DataLoader {

  def loadLocalFile(dataResourceUid:String, filePath:String, logRowKeys:Boolean=true, testFile:Boolean=false){
    retrieveConnectionParameters(dataResourceUid) match {
      case None => logger.error("Unable to retrieve connection details for " + dataResourceUid)
      case Some(config) =>
        val strip = config.connectionParams.getOrElse("strip", false).asInstanceOf[Boolean]
        val incremental = config.connectionParams.getOrElse("incremental",false).asInstanceOf[Boolean]
        loadFile(new File(filePath), dataResourceUid, config.uniqueTerms, config.connectionParams, strip, incremental || logRowKeys, testFile)
    }
  }

  def load(dataResourceUid:String, logRowKeys:Boolean=true, testFile:Boolean=false, forceLoad:Boolean=false, removeNullFields:Boolean=false){
    //remove the old files
    emptyTempFileStore(dataResourceUid)
    //delete the old file
    deleteOldRowKeys(dataResourceUid)

    retrieveConnectionParameters(dataResourceUid) match {
      case None => logger.error("Unable to retrieve connection details for " + dataResourceUid)
      case Some(config) =>
        val strip = config.connectionParams.getOrElse("strip", false).asInstanceOf[Boolean]
        val incremental = config.connectionParams.getOrElse("incremental",false).asInstanceOf[Boolean]
        var loaded = false
        var maxLastModifiedDate:java.util.Date = null
        config.urls.foreach { url =>
          val (fileName, date) = downloadArchive(url, dataResourceUid, if(forceLoad) None else config.dateLastChecked)
          if(maxLastModifiedDate == null || date.after(maxLastModifiedDate)) {
            maxLastModifiedDate = date
          }
          logger.info("File last modified date: " + maxLastModifiedDate)
          if(fileName != null){
            val directory = new File(fileName)
            loadDirectory(directory, dataResourceUid, config.uniqueTerms, config.connectionParams, strip, incremental || logRowKeys, testFile,  removeNullFields)
            loaded = true
          }
        }
        //now update the last checked and if necessary data currency dates
        if(!testFile){
          updateLastChecked(dataResourceUid, if(loaded) Some(maxLastModifiedDate) else None)
          if(!loaded) {
            setNotLoadedForOtherPhases(dataResourceUid)
          }
        }
    }
  }

  //loads all the files in the subdirectories that are not multimedia
  def loadDirectory(directory:File, dataResourceUid:String, uniqueTerms:Seq[String], params:Map[String,String],
                    stripSpaces:Boolean=false, logRowKeys:Boolean=true, test:Boolean=false, deleteIfNullValue:Boolean=false){

    directory.listFiles.foreach(file => {
      if(file.isFile()&& !Config.mediaStore.isMediaFile(file)) {
        loadFile(file, dataResourceUid, uniqueTerms, params, stripSpaces, logRowKeys, test, deleteIfNullValue)
      } else if(file.isDirectory) {
        loadDirectory(file, dataResourceUid, uniqueTerms, params, stripSpaces, logRowKeys, test, deleteIfNullValue)
      } else {
        logger.warn("Unable to load as CSV: " + file.getAbsolutePath())
      }
    })
  }

  /**
   * Load the supplied file of CSV data for the supplied data resource.
   *
   * @param file
   * @param dataResourceUid
   * @param uniqueTerms
   * @param params
   * @param stripSpaces
   * @param logRowKeys
   * @param test
   */
  def loadFile(file:File, dataResourceUid:String, uniqueTerms:Seq[String], params:Map[String,String],
               stripSpaces:Boolean=false, logRowKeys:Boolean=true, test:Boolean = false, deleteIfNullValue:Boolean=false){

    val rowKeyWriter = getRowKeyWriter(dataResourceUid, logRowKeys)

    val quotechar = params.getOrElse("csv_text_enclosure", "\"").head
    val separator = {
      val separatorString = params.getOrElse("csv_delimiter", ",")
      if (separatorString == "\\t") '\t'
      else separatorString.toCharArray.head
    }
    val escape = params.getOrElse("csv_escape_char","\\").head
    val reader = new CSVReader(new InputStreamReader(new org.apache.commons.io.input.BOMInputStream(new FileInputStream(file))), separator, quotechar, escape)

    logger.info("Using CSV reader with the following settings quotes: " + quotechar + " separator: " + separator + " escape: " + escape)
    //match the column headers to dwc terms
    val dwcTermHeaders = mapHeadersToDwC(reader)

    if(dwcTermHeaders == null){
      logger.warn("No headers or content in file.")
      return
    }

    var currentLine = reader.readNext

    if (currentLine == null) {
      logger.warn("No content in file.")
      return
    }

    if(uniqueTerms.isEmpty){
      logger.warn("No unique terms provided for this data resource !")
    } else {
      logger.info("Unique terms: " + uniqueTerms.mkString(","))
    }

    logger.info("Column headers: " + dwcTermHeaders.mkString(","))

    val validConfig = uniqueTerms.isEmpty || uniqueTerms.forall(t => dwcTermHeaders.contains(t))
    if(!validConfig){
      throw new RuntimeException("Bad configuration for file: "+ file.getName + " for resource: " +
        dataResourceUid + ". CSV file is missing unique terms.")
    }


    val newCollCodes = new scala.collection.mutable.HashSet[String]
    val newInstCodes = new scala.collection.mutable.HashSet[String]

    var counter = 1
    var newCount = 0
    var noSkipped = 0
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis

    while(currentLine != null){

      counter += 1

      val columns = currentLine.toList
      if (columns.length >= dwcTermHeaders.size - 1){

        //create the map of properties
        val map = (dwcTermHeaders zip columns).toMap[String,String].filter {
          case (key,value) => {
            if(value != null ){
              if(value.trim != ""){
                true
              } else {
                deleteIfNullValue  // add the pair if the empty values will be removed
              }
            } else {
              deleteIfNullValue  // add the pair if the null values will be removed
            }
          }
        }

        //only continue if there is at least one non-null unique term
        if(uniqueTerms.find(t => map.getOrElse(t, "").length > 0).isDefined || uniqueTerms.length == 0){

          val uniqueTermsValues = uniqueTerms.map(t => map.getOrElse(t,""))

          if(test){
            newInstCodes.add(map.getOrElse("institutionCode", "<NULL>"))
            newCollCodes.add(map.getOrElse("collectionCode", "<NULL>"))
          }
          val (uuid, isnew) = Config.occurrenceDAO.createOrRetrieveUuid(Config.occurrenceDAO.createUniqueID(dataResourceUid, uniqueTermsValues, stripSpaces))
          if(isnew) {
            newCount += 1
          }

          if(!test){

            val mapToUse = meddle(map)

            val fr = FullRecordMapper.createFullRecord("", mapToUse, Versions.RAW)

            if (fr.occurrence.associatedMedia != null){
              //check for full resolvable http paths
              val filesToImport = fr.occurrence.associatedMedia.split(";")
              val filePathsInStore = filesToImport.map { name =>
                //if the file name isnt a HTTP URL construct file absolute file paths
                val fileName = name.trim();
                if(!fileName.startsWith("http://") && !fileName.startsWith("https://") && !fileName.startsWith("ftp://")  && !fileName.startsWith("ftps://")  && !fileName.startsWith("file://")){
                  val filePathBuffer = new ArrayBuffer[String]
                  filePathBuffer += "file:///" + file.getParent + File.separator + fileName

                  //do multiple formats exist? check for files of the same name, different extension
                  val directory = file.getParentFile
                  val differentFormats = directory.listFiles(new SameNameDifferentExtensionFilter(fileName))
                  differentFormats.foreach { file =>
                    filePathBuffer += "file:///" + file.getParent + File.separator + file.getName
                  }

                  filePathBuffer.toArray[String]
                } else {
                  Array(fileName)
                }
              }.flatten
              logger.info("Loading: " + filePathsInStore.mkString("; "))
              fr.occurrence.associatedMedia = filePathsInStore.mkString(";")
            }
            load(dataResourceUid, fr, uniqueTermsValues, true, false, stripSpaces, rowKeyWriter, List(), deleteIfNullValue)
          }

          if (counter % 1000 == 0 && counter > 0) {
            finishTime = System.currentTimeMillis
            logger.info(counter + ", >> last key : " + dataResourceUid + "|" +
              uniqueTermsValues.mkString("|") + ", records per sec: " +
              1000 / (((finishTime - startTime).toFloat) / 1000f))
            startTime = System.currentTimeMillis
          }
        } else {
          noSkipped += 1
          logger.warn("Skipping line: " + counter + ", missing unique term value. Number skipped: "+ noSkipped)
          uniqueTerms.foreach(t => logger.info("Unique term: " + t + " : " + map.getOrElse(t,"")))
        }
      } else {
        logger.warn("Skipping line: " +counter + " incorrect number of columns (" +
          columns.length + ")...headers (" + dwcTermHeaders.length + ")")
        logger.info("First element : "+columns(0) +"...headers :" + dwcTermHeaders(0))
        logger.info("last element : "+columns.last +"...headers :" + dwcTermHeaders.last)
      }
      //read next
      currentLine = reader.readNext
    }

    if(rowKeyWriter.isDefined){
      rowKeyWriter.get.flush
      rowKeyWriter.get.close
    }

    //check to see if the inst/coll codes are new
    if(test){

      val institutionCodes = Config.indexDAO.getDistinctValues("data_resource_uid:"+dataResourceUid, "institution_code",100).getOrElse(List()).toSet[String]
      val collectionCodes = Config.indexDAO.getDistinctValues("data_resource_uid:"+dataResourceUid, "collection_code",100).getOrElse(List()).toSet[String]
      logger.info("The current institution codes for the data resource: " + institutionCodes)
      logger.info("The current collection codes for the data resource: " + collectionCodes)

      val unknownInstitutions = newInstCodes &~ institutionCodes
      val unknownCollections = newCollCodes &~ collectionCodes
      if(!unknownInstitutions.isEmpty) {
        logger.warn("Warning there are new institution codes in the set. " + unknownInstitutions)
      }
      if(!unknownCollections.isEmpty) {
        logger.warn("Warning there are new collection codes in the set. " + unknownCollections)
      }
    }
    logger.info("There are " + counter + " records in the file. The number of NEW records: " + newCount)
    logger.info("Load finished for " + file.getName())
  }

  /**
   *
   *
   * @param map
   * @return
   */
  def meddle(map:Map[String, String]) = map

  /**
   *
   * @param reader
   * @return
   */
  def mapHeadersToDwC(reader: CSVReader): Seq[String] = {
    val headerLine = reader.readNext
    if (headerLine != null) {
      val columnHeaders = headerLine.map(t => t.replace(" ", "").trim).toList
      DwC.retrieveCanonicals(columnHeaders)
    } else {
      null
    }
  }
}
