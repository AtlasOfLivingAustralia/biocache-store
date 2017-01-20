package au.org.ala.biocache.load

import java.net.URL

import au.org.ala.biocache.util.{BiocacheConversions, FileHelper, HttpUtil, SFTPTools}
import org.slf4j.LoggerFactory
import au.org.ala.biocache.Config
import org.apache.commons.io.{FileUtils, FilenameUtils}
import java.io.{File, FileOutputStream, Writer}

import scala.util.parsing.json.JSON
import java.util.Date

import au.org.ala.biocache.parser.DateParser
import org.gbif.dwc.terms.TermFactory

import scala.collection.mutable.ArrayBuffer
import au.org.ala.biocache.model.{FullRecord, Multimedia}

import scala.collection.mutable

/**
 * A trait with utility code for loading data into the occurrence store.
 */
trait DataLoader {

  import BiocacheConversions._
  import FileHelper._

  val user = "biocache"
  val logger = LoggerFactory.getLogger("DataLoader")
  val temporaryFileStore = Config.loadFileStore
  val pm = Config.persistenceManager
  val loadTime = org.apache.commons.lang.time.DateFormatUtils.format(new java.util.Date, "yyyy-MM-dd'T'HH:mm:ss'Z'")
  val sftpPattern = """sftp://([a-zA-z\.]*):([0-9a-zA-Z_/\.\-]*)""".r

  def emptyTempFileStore(resourceUid:String) = {
    FileUtils.deleteQuietly(new File(temporaryFileStore + File.separator + resourceUid))
  }

  /**
   * Returns the file writer to be used to store the row keys that need to be deleted for a data resource
   * @param resourceUid
   * @return
   */
  def getDeletedFileWriter(resourceUid:String):java.io.FileWriter ={
    val file =  new File(Config.deletedFileStore +File.separator + resourceUid+File.separator+"deleted.txt")
    FileUtils.forceMkdir(file.getParentFile)
    new java.io.FileWriter(file)
  }

  def deleteOldRowKeys(resourceUid:String){
    //delete the row key file so that it only exists if the load is configured to
    //thus processing and indexing of the data resource should check to see if a file exists first
    FileUtils.deleteQuietly(new File(Config.tmpWorkDir + "/row_key_"+resourceUid+".csv"))
  }

  def getRowKeyWriter(resourceUid:String, writeRowKeys:Boolean):Option[java.io.Writer]={
    if(writeRowKeys){
      FileUtils.forceMkdir(new File(Config.tmpWorkDir))
      //the file is deleted first so we set it up to append.  allows resources with multiple files to have row keys recorded
      Some(new java.io.FileWriter(Config.tmpWorkDir + "/row_key_"+resourceUid+".csv", true))
    } else {
      None
    }
  }

  /**
   * Sampling, Processing and Indexing look for the row key file.
   * An empty file should be enough to prevent the phase from going ahead...
   */
  def setNotLoadedForOtherPhases(resourceUid:String){
    def writer = getRowKeyWriter(resourceUid, true)
    if(writer.isDefined){
      writer.get.flush
      writer.get.close
    }
  }

  def getDataResourceDetailsAsMap(uid:String) : Map[String, String] = {
    val json = scala.io.Source.fromURL(Config.registryUrl + "/dataResource/" + uid, "UTF-8").getLines().mkString
    JSON.parseFull(json).get.asInstanceOf[Map[String, String]]
  }

  def getDataProviderDetailsAsMap(uid:String) : Map[String, String] = {
    val json = scala.io.Source.fromURL(Config.registryUrl + "/dataProvider/" + uid, "UTF-8").getLines().mkString
    JSON.parseFull(json).get.asInstanceOf[Map[String, String]]
  }

  def getInstitutionDetailsAsMap(uid:String) : Map[String, String] = {
    val json = scala.io.Source.fromURL(Config.registryUrl + "/institution/" + uid, "UTF-8").getLines().mkString
    JSON.parseFull(json).get.asInstanceOf[Map[String, String]]
  }

  /**
   * Retrieve the connection parameters for the supplied resource UID.
   *
   * @param resourceUid
   * @return
   */
  def retrieveConnectionParameters(resourceUid: String) : Option[DataResourceConfig] = try {

    //full document
    val map = getDataResourceDetailsAsMap(resourceUid)

    //connection details
    val connectionParameters = map.getOrElse("connectionParameters", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]

    //protocol
    val protocol: String = connectionParameters.getOrElse("protocol", "").asInstanceOf[String]
    val urlsObject = connectionParameters.getOrElse("url", List[String]())
    val urls = {
      if (urlsObject.isInstanceOf[List[_]]) {
        urlsObject
      } else {
        val singleValue = connectionParameters("url").asInstanceOf[String]
        List(singleValue)
      }
    }

    //retrieve the unique terms for this data resource
    val uniqueTerms = connectionParameters.get("termsForUniqueKey") match {
      case Some(list: List[String]) => list
      case Some(singleValue: String) => List(singleValue)
      case None => List[String]()
    }

    //optional config params for custom services
    val customParams = protocol.asInstanceOf[String].toLowerCase match {
      // Only current data resource using this is dr710
      case "customwebservice" => {
        val params = connectionParameters.getOrElse("params", "").asInstanceOf[String]
        JSON.parseFull(params).getOrElse(Map[String, String]()).asInstanceOf[Map[String, String]]
      }
      case _ => Map[String, String]()
    }

    //last checked date
    val lastChecked = map("lastChecked").asInstanceOf[String]
    val dateLastChecked = DateParser.parseStringToDate(lastChecked)

    //return the config
    Some(new DataResourceConfig(protocol,
      urls.asInstanceOf[List[String]],
      uniqueTerms,
      connectionParameters.asInstanceOf[Map[String, String]],
      customParams,
      dateLastChecked)
    )
  } catch {
    case e:Exception =>
      logger.error(s"Problem retrieve data resource config for resource: $resourceUid", e)
      None
  }

  def mapConceptTerms(terms: Seq[String]): Seq[org.gbif.dwc.terms.Term] = {
    val termFactory = TermFactory.instance()
    terms.map(term => termFactory.findTerm(term))
  }

  def exists(dataResourceUid:String, identifyingTerms:List[String]) : Boolean = {
    !Config.occurrenceDAO.getUUIDForUniqueID(createUniqueID(dataResourceUid, identifyingTerms)).isEmpty
  }

  /**
   * Creates a unique key for this record using the unique terms for this data resource.
   *
   * @param dataResourceUid
   * @param identifyingTerms
   * @param stripSpaces
   * @return
   */
  protected def createUniqueID(dataResourceUid:String, identifyingTerms:Seq[String], stripSpaces:Boolean=false) : String = {
    val uniqueId = (List(dataResourceUid) ::: identifyingTerms.toList).mkString("|").trim
    if(stripSpaces)
      uniqueId.replaceAll("\\s","")
    else
      uniqueId
  }

  def load(dataResourceUid:String, fr:FullRecord, identifyingTerms:Seq[String], multimedia:Seq[Multimedia]) : Boolean = {
    load(dataResourceUid:String, fr:FullRecord, identifyingTerms:Seq[String], true, false, false, None, multimedia, false)
  }

  def load(dataResourceUid:String, fr:FullRecord, identifyingTerms:Seq[String]) : Boolean = {
    load(dataResourceUid:String, fr:FullRecord, identifyingTerms:Seq[String], true, false, false, None, List(), false)
  }

  def load(dataResourceUid:String, fr:FullRecord, identifyingTerms:Seq[String], updateLastModified:Boolean) : Boolean = {
    load(dataResourceUid:String, fr:FullRecord, identifyingTerms:Seq[String], updateLastModified, false, false, None, List(), false)
  }

  def load(dataResourceUid: String, fr: FullRecord, identifyingTerms: Seq[String], updateLastModified: Boolean, downloadMedia: Boolean, deleteIfNullValue: Boolean):Boolean ={
    load(dataResourceUid, fr, identifyingTerms, updateLastModified, downloadMedia, false, None, List(), deleteIfNullValue)
  }

  /**
   * Load a record into the occurrence store.
   *
   * @param dataResourceUid the data resource UID
   * @param fr a representation of the raw record
   * @param identifyingTerms
   * @param updateLastModified
   * @param downloadMedia whether to download the referenced media
   * @param stripSpaces whether to strip spaces from the identifying terms.
   * @param rowKeyWriter
   * @return
   */
  def load(dataResourceUid: String, fr: FullRecord, identifyingTerms: Seq[String], updateLastModified: Boolean, downloadMedia: Boolean, stripSpaces: Boolean, rowKeyWriter: Option[Writer], multimedia: Seq[Multimedia], deleteIfNullValue: Boolean): Boolean = {

    //the details of how to construct the UniqueID belong in the Collectory
    val uniqueID = if(identifyingTerms.isEmpty) {
      None
    } else {
      Some(createUniqueID(dataResourceUid, identifyingTerms, stripSpaces))
    }

    //lookup the column
    val (recordUuid, isNew) = {
      if(fr.uuid != null && fr.uuid.trim != ""){
        (fr.uuid, false)
      } else {
        uniqueID match {
          case Some(value) => Config.occurrenceDAO.createOrRetrieveUuid(value)
          case None => (Config.occurrenceDAO.createUuid, true)
        }
      }
    }

    //add the full record
    fr.uuid = recordUuid
    //The row key is the uniqueID for the record. This will always start with the dataResourceUid
    fr.rowKey = if(uniqueID.isEmpty) {
      dataResourceUid + "|" + recordUuid
    } else {
      uniqueID.get
    }

    //write the rowkey to file if a writer is provided. allows large data resources to be
    //incrementally updated and only process/index changes
    if(rowKeyWriter.isDefined){
      rowKeyWriter.get.write(fr.rowKey+"\n")
    }

    //The last load time
    if(updateLastModified){
      fr.lastModifiedTime = loadTime
    }

    //set first loaded date indicating when this record was first loaded
    if(isNew){
      fr.firstLoaded = loadTime
    }

    fr.attribution.dataResourceUid = dataResourceUid

    //process the media for this record
    processMedia(dataResourceUid, fr, multimedia)

    //load the record
    Config.occurrenceDAO.addRawOccurrence(fr,deleteIfNullValue)
    true
  }

  /**
   * Load the media where possible.
   *
   * @param dataResourceUid
   * @param fr
   * @param multimedia An optional list of multimedia information derived from other sources
   */
  def processMedia(dataResourceUid: String, fr: FullRecord, multimedia: Seq[Multimedia] = Seq.empty[Multimedia]) : FullRecord = {

    //download the media - checking if it exists already
    //supplied media comes from a separate source. If it's also listed in the associatedMedia then don't double-load it
    val suppliedMedia = multimedia map { media => media.location.toString }
    val associatedMedia = DownloadMedia.unpackAssociatedMedia(fr.occurrence.associatedMedia).filter { url =>
      suppliedMedia.forall(!_.endsWith(url))
    }

    val filesToImport = (associatedMedia ++ suppliedMedia).filter { url =>
      Config.blacklistedMediaUrls.forall(!url.startsWith(_))
    }

    if (filesToImport.isEmpty) {
      return fr
    }

    val fileNameToID = new mutable.HashMap[String, String]()

    val associatedMediaBuffer = new ArrayBuffer[String]
    val imagesBuffer = new ArrayBuffer[String]
    val soundsBuffer = new ArrayBuffer[String]
    val videosBuffer = new ArrayBuffer[String]

    filesToImport.foreach { fileToStore =>

      val media = {
        val multiMediaObject = multimedia.find { media => media.location.toString == fileToStore }
        multiMediaObject match {
          case Some(multimedia) => Some(multimedia)
          case None => {
            //construct metadata from record
            Some(new Multimedia(new URL(fileToStore), "", Map(
              "creator" -> fr.occurrence.recordedBy,
              "title" -> fr.classification.scientificName,
              "description" -> fr.occurrence.occurrenceRemarks,
              "license" -> fr.occurrence.license,
              "rights" -> fr.occurrence.rights,
              "rightsHolder" -> fr.occurrence.rightsholder
            )))
          }
        }
      }

      // save() checks to see if the media has already been stored
      val savedTo = Config.mediaStore.save(fr.uuid, fr.attribution.dataResourceUid, fileToStore, media)
      savedTo match {
        case Some((savedFilename, savedFilePathOrId)) => {
          if (Config.mediaStore.isValidSound(fileToStore)) {
            soundsBuffer += savedFilePathOrId
          } else if (Config.mediaStore.isValidVideo(fileToStore)) {
            videosBuffer += savedFilePathOrId
          } else {
            imagesBuffer += savedFilePathOrId
          }
          associatedMediaBuffer += savedFilename
        }
        case None => logger.warn("Unable to save file: " + fileToStore)
      }

      //add the references
      fr.occurrence.associatedMedia = associatedMediaBuffer.toArray.mkString(";")
      fr.occurrence.images = imagesBuffer.toArray
      fr.occurrence.sounds = soundsBuffer.toArray
      fr.occurrence.videos = videosBuffer.toArray
    }
    fr
  }

  /**
   * Download an archive from the supplied URL. Includes support for downloading from
   * SFTP server.
   * 
   * @param url
   * @param resourceUid
   * @param lastChecked
   * @return
   */
  protected def downloadArchive(url:String, resourceUid:String, lastChecked:Option[Date]) : (String,Date) = {
    //when the url starts with SFTP need to SCP the file from the supplied server.
    val (file, date, isZipped, isGzipped) = if (url.startsWith("sftp://")){
      downloadSFTPArchive(url, resourceUid, lastChecked)
    } else {
      downloadStandardArchive(url, resourceUid, lastChecked)
    }
    if(file != null){
    //extract the file
      if (isZipped){
        logger.info("Extracting ZIP " + file.getAbsolutePath)
        file.extractZip
        val fileName = FilenameUtils.removeExtension(file.getAbsolutePath)
        logger.info("Archive extracted to directory: " + fileName)
        (fileName, date)
      } else if (isGzipped){
        logger.info("Extracting GZIP " + file.getAbsolutePath)
        file.extractGzip
        //need to remove the gzip file so the loader doesn't attempt to load it.
        FileUtils.forceDelete(file)
        val fileName = FilenameUtils.removeExtension(file.getAbsolutePath)
        logger.info("Archive extracted to directory: " + fileName)
        ((new File(fileName)).getParentFile.getAbsolutePath,date)
      } else {
        (file.getParentFile.getAbsolutePath, date)
      }
    } else {
      logger.info(s"Unable to extract a new file for $resourceUid at $url")
      (null,null)
    }
  }

  /**
   * Download the archive from an SFTP endpoint.
   *
   * @param url
   * @param resourceUid
   * @param lastChecked
   * @return
   */
  protected def downloadSFTPArchive(url:String, resourceUid:String, lastChecked:Option[Date]) : (File, Date, Boolean, Boolean) = {
    url match {
      case sftpPattern(server, filename) => {
        val (targetfile, date, isZipped, isGzipped, downloaded) = {
        if (url.endsWith(".zip") ){
          val f = new File(temporaryFileStore + resourceUid + ".zip")
          f.createNewFile()
          (f,null, true, false,false)
        } else if (url.endsWith(".gz")){
          val f = new File(temporaryFileStore + resourceUid + File.separator + resourceUid +".gz")
          logger.info("  creating file: " + f.getAbsolutePath)
          FileUtils.forceMkdir(f.getParentFile())
          f.createNewFile()
          (f,null, false, true,false)
        } else if (filename.contains(".")) {
          val f = new File(temporaryFileStore + resourceUid + File.separator + resourceUid +".csv")
          logger.info("  creating file: " + f.getAbsolutePath)
          FileUtils.forceMkdir(f.getParentFile())
          f.createNewFile()
          (f,null, false, false,false)
        } else {
          logger.info("SFTP the most recent from " + url)
          def fileDetails = SFTPTools.sftpLatestArchive(url, resourceUid, temporaryFileStore,lastChecked)
          if(fileDetails.isDefined){
            val (file, date) = fileDetails.get
            logger.info(s"The most recent file is $file with last modified date : $date")
            (new File(file),date,file.endsWith("zip"),file.endsWith("gz"),true)
          } else {
            (null, null, false, false, false)
          }
        }}

        val fileDetails = if(targetfile == null) {
          None
        } else if(!downloaded){
          SFTPTools.scpFile(
            server,
            Config.getProperty("uploadUser"),
            Config.getProperty("uploadPassword"),
            filename,
            targetfile)
        } else {
          Some ((targetfile, date) )
        }

        if(fileDetails.isDefined){
          val (file, date) = fileDetails.get
          (targetfile, date, isZipped, isGzipped)
        } else {
          (null, null, false, false)
        }
      }
      case _ => (null, null, false, false)
    }
  }

  /**
   * Retrieve details of the latest archive after the supplied date.
   *
   * @param url
   * @param resourceUid
   * @param afterDate
   * @return
   */
  def sftpLatestArchive(url:String, resourceUid:String, afterDate:Option[Date]) : Option[(String, Date)] =
    SFTPTools.sftpLatestArchive(url, resourceUid, temporaryFileStore,afterDate)

  /**
   * Download an archive from the supplied URL.
   * 
   * @param url
   * @param resourceUid
   * @param afterDate
   * @return
   */
  def downloadStandardArchive(url:String, resourceUid:String, afterDate:Option[Date]) : (File, Date, Boolean, Boolean) = {
    
    val tmpStore = new File(temporaryFileStore)
    if(!tmpStore.exists){
      FileUtils.forceMkdir(tmpStore)
    }

    logger.info("Downloading zip file from "+ url)
    val urlConnection = new java.net.URL(url.replaceAll(" " ,"%20")).openConnection()
    val date = if(urlConnection.getLastModified() == 0) new Date() else new Date(urlConnection.getLastModified())
    //logger.info("URL Last Modified: " +urlConnection.getLastModified())
    if(afterDate.isEmpty || urlConnection.getLastModified() == 0 || afterDate.get.getTime() < urlConnection.getLastModified()){
      //handle the situation where the files name is not supplied in the URL but in the Content-Disposition
      val contentDisp = urlConnection.getHeaderField("Content-Disposition")
      if(contentDisp != null){
          logger.info(" Content-Disposition: " + contentDisp)
      }
      val in = urlConnection.getInputStream()
      val (file, isZipped, isGzipped) = {
        if (url.endsWith(".zip") || (contentDisp != null && contentDisp.endsWith(""".zip""""))){
          val f = new File(temporaryFileStore + File.separatorChar + resourceUid + ".zip")
          logger.info("Creating file: " + f.getAbsolutePath)
          f.createNewFile()
          (f, true, false)
        } else if (url.endsWith(".gz") || (contentDisp != null && contentDisp.endsWith(""".gz""""))){
          val f = new File(temporaryFileStore  + File.separatorChar + resourceUid + File.separator + resourceUid +".gz")
          logger.info("Creating file: " + f.getAbsolutePath)
          FileUtils.forceMkdir(f.getParentFile())
          f.createNewFile()
          (f, false, true)
        } else {
          val f = new File(temporaryFileStore + File.separatorChar + resourceUid + File.separator + resourceUid +".csv")
          logger.info("Creating file: " + f.getAbsolutePath)
          FileUtils.forceMkdir(f.getParentFile())
          f.createNewFile()
          (f, false, false)
        }
      }
      val out = new FileOutputStream(file)
      val buffer: Array[Byte] = new Array[Byte](40960)
      var numRead = 0
      var counter = 0
      while ({ numRead = in.read(buffer); numRead != -1 }) {
        counter += numRead
        out.write(buffer, 0, numRead)
        out.flush
      }
      out.flush
      in.close
      out.close
      logger.info("Downloaded. File size: ", counter / 1024 +"kB, " + file.getAbsolutePath +", is zipped: " + isZipped+"\n")
      (file,date,isZipped,isGzipped)
    } else {
      logger.info("The file has not changed since the last time it  was loaded. " +
        "To load the data a force-load will need to be performed")
      (null,null,false,false)
    }
  }

  /**
   * Calls the collectory webservice to update the last loaded time for a data resource
   */
  def updateLastChecked(resourceUid:String, dataCurrency:Option[Date] = None) : Boolean ={
    try {
      //set the last check time for the supplied resourceUid only if configured to allow updates
      if(Config.allowCollectoryUpdates == "true"){

        val map = new  scala.collection.mutable.HashMap[String,String]()
        map ++= Map("user"-> user, "api_key"-> Config.collectoryApiKey, "lastChecked" -> loadTime)

        if(dataCurrency.isDefined) {
          map += ("dataCurrency" -> dataCurrency.get)
        }
        //turn the map of values into JSON representation
        val data = map.map(pair => "\""+pair._1 +"\":\"" +pair._2 +"\"").mkString("{",",", "}")

        val (responseCode, responseBody) = HttpUtil.postBody(Config.registryUrl + "/dataResource/" + resourceUid, "application/json", data)

        logger.info("Registry response code: " + responseCode)
      }
      true
    } catch {
      case e:Exception => logger.warn("Unable to update the lastChecked timestamp in the collectory. " +
        " This is most likely caused by a bad URL" +
        " path for the collectory. Please check configuration. " + e.getMessage, e); false
   }
  }
}
