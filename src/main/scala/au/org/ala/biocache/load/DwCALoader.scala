package au.org.ala.biocache.load

import java.io._
import java.net.URL

import au.org.ala.biocache._
import au.org.ala.biocache.cmd.{CMD2, NoArgsTool, Tool}
import au.org.ala.biocache.model.{FullRecord, Multimedia, Raw}
import au.org.ala.biocache.util.OptionParser
import au.org.ala.biocache.vocab.DwC
import org.apache.commons.lang3.StringUtils
import org.gbif.dwca.record.{Record, StarRecord}
import org.gbif.dwc.terms.{DcTerm, GbifTerm, Term}
import org.gbif.dwca.io.ArchiveFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.{JavaConversions, mutable}

/**
 * Loading utility for pulling in a darwin core archive file.
 *
 * This class will retrieve details from the collectory and load in a darwin core archive.
 * The workflow is the following:
 *
 * <ol>
 * <li>Retrieve JSON from registry</li>
 * <li>Download the zipped archive to the local file system</li>
 * <li>Extract the archive on the local filesystem</li>
 * <li>Load the data into the occurrence store (e.g. Cassandra)</li>
 * </ol>
 *
 * Optimisations - with a significant memory allocation, this loader _could_ retrieve all
 * uniqueKey to UUID mappings first.
 *
 * This makes this class completely reliant upon configuration in the collectory.
 *
 * @author Dave Martin
 */
object DwCALoader extends Tool {

  val IMAGE_TYPE = GbifTerm.Image
  val MULTIMEDIA_TYPE = GbifTerm.Multimedia
//  val IDENTIFICATION_TYPE = GbifTerm.

  def cmd = "load-dwca"
  def desc = "Load a Darwin Core Archive"

  def main(args: Array[String]): Unit = {

    var resourceUid = ""
    var localFilePath:Option[String] = None
    var logRowKeys = false
    var testFile = false
    var bypassConnParamLookup = false
    var removeNullFields = false
    val parser = new OptionParser("load darwin core archive") {
      arg("<data resource UID>", "The UID of the data resource to load", { v: String => resourceUid = v })
      opt("l", "local", "skip the download and use local file", { v:String => localFilePath = Some(v) } )
      booleanOpt("b", "bypassConnParamLookup", "bypass connection parameter lookup in the registry (collectory)", {
        v:Boolean => bypassConnParamLookup = v }
      )
      opt("log","log row keys to file - allows processing/indexing of changed records",{ logRowKeys = true })
      opt("test", "test the file only do not load", { testFile = true })
      opt("rnf", "remove-null-fields", "Remove the null/Empty fields currently exist in the atlas", { removeNullFields = true })

    }
    if(parser.parse(args)){
      val l = new DwCALoader
      l.deleteOldRowKeys(resourceUid)
      if(localFilePath.isEmpty){
        l.load(resourceUid, logRowKeys, testFile)
      } else {
        if(bypassConnParamLookup){
          l.loadArchive(localFilePath.get, resourceUid, List(), None, false, logRowKeys, testFile, removeNullFields)
        } else {
          l.loadLocal(resourceUid, localFilePath.get, logRowKeys,testFile)
        }
      }
      //initialise the delete & update the collectory information
      l.updateLastChecked(resourceUid)
    }

    //shut down the persistence manager after all the files have been loaded.
//    Config.persistenceManager.shutdown
  }
}

class DwCALoader extends DataLoader {

  import JavaConversions._

  /**
   * Load a resource
   *
   * @param resourceUid
   * @param logRowKeys
   * @param testFile
   * @param forceLoad
   */
  def load(resourceUid:String, logRowKeys:Boolean=false, testFile:Boolean=false, forceLoad:Boolean = false, removeNullFields:Boolean=false){
    //remove the old files
    emptyTempFileStore(resourceUid)
    //remove the old row keys:
    deleteOldRowKeys(resourceUid)
    retrieveConnectionParameters(resourceUid) match {
      case None => throw new Exception("Unable to retrieve connection params for " + resourceUid)
      case Some(dataResourceConfig) =>
        val conceptTerms = mapConceptTerms(dataResourceConfig.uniqueTerms)
        val imageUrl = dataResourceConfig.connectionParams.get("imageUrl")
        val incremental = dataResourceConfig.connectionParams.getOrElse("incremental", false).asInstanceOf[Boolean]
        val strip = dataResourceConfig.connectionParams.getOrElse("strip", false).asInstanceOf[Boolean]
        var loaded = false
        var maxLastModifiedDate:java.util.Date = null
        dataResourceConfig.urls.foreach { url =>
          //download
          val (fileName,date) = downloadArchive(url,resourceUid, if(forceLoad) None else dataResourceConfig.dateLastChecked)
          if(maxLastModifiedDate == null || date.after(maxLastModifiedDate)){
            maxLastModifiedDate = date
          }
          logger.info("File last modified date: " + maxLastModifiedDate)
          if(fileName != null){
            //load the DWC file
            loadArchive(fileName, resourceUid, conceptTerms, imageUrl, strip, logRowKeys||incremental, testFile, removeNullFields)
            loaded = true
          }
        }
        //now update the last checked and if necessary data currency dates
        if(!testFile){
          updateLastChecked(resourceUid, if(loaded) Some(maxLastModifiedDate) else None)
          if(!loaded){
            setNotLoadedForOtherPhases(resourceUid)
          }
        }
    }
  }

  def loadLocal(resourceUid:String, fileName:String, logRowKeys:Boolean, testFile:Boolean, removeNullFields:Boolean=false){
    retrieveConnectionParameters(resourceUid) match {
      case None => throw new Exception("Unable to load resourceUid: " + resourceUid)
      case Some(dataResourceConfig) =>
        val conceptTerms = mapConceptTerms(dataResourceConfig.uniqueTerms)
        val strip = dataResourceConfig.connectionParams.getOrElse("strip", false).asInstanceOf[Boolean]
        //load the DWC file
        loadArchive(fileName, resourceUid, conceptTerms, dataResourceConfig.connectionParams.get("imageUrl"), strip, logRowKeys, testFile,removeNullFields)
    }
  }

  def getUuid(uniqueID:Option[String], star:StarRecord, uniqueTerms:Seq[Term], mappedProperties:Option[Map[String,String]]) : ((String, Boolean), Option[Map[String,String]]) = {
    uniqueID match {
      case Some(value) => (Config.occurrenceDAO.createOrRetrieveUuid(value),mappedProperties)
      case None => ((Config.occurrenceDAO.createUuid, true), mappedProperties)
    }
  }

  def loadArchive(fileName:String, resourceUid:String, uniqueTerms:Seq[Term], imageUrl:Option[String], stripSpaces:Boolean, logRowKeys:Boolean, testFile:Boolean, removeNullFields:Boolean=false){
    logger.info(s"Loading archive: $fileName " +
      s"for resource: $resourceUid, " +
      s"with unique terms: $uniqueTerms, " +
      s"stripping spaces:  $stripSpaces, " +
      s"incremental: $logRowKeys,  " +
      s"testing: $testFile")

    val archiveDir = new File(fileName)
    val imageBase = new URL(imageUrl getOrElse archiveDir.toURI.toURL.toString)
    val rowKeyWriter = getRowKeyWriter(resourceUid, logRowKeys)
    val archive = ArchiveFactory.openArchive(archiveDir)
    val iter = archive.iterator()
    var count = 0
    var newCount = 0

    val fieldMap = archive.getCore().getFields()

    val fieldShortNames = fieldMap.keySet().toList
    val biocacheModelValues = DwC.retrieveCanonicals(fieldShortNames.map(_.simpleName))

    //constructs a map of supplied field name -> biocache model property
    val fieldToModelMap = (fieldShortNames zip biocacheModelValues).toMap

    //constructs a map of supplied field IDX -> biocache model property
    val fieldShortNameToIdxMap = new mutable.HashMap[Int, String]

    fieldMap.foreach({ case (term, field) =>
      val fieldIdx = field.getIndex()
      if(fieldIdx != null) {
        DwC.matchTerm(term.simpleName) match {
          case Some(matchedTerm) => fieldShortNameToIdxMap.put(fieldIdx, matchedTerm.canonical)
          case None => fieldShortNameToIdxMap.put(fieldIdx, term.simpleName)
        }
      }
    })

    if(logger.isDebugEnabled){
      fieldToModelMap.foreach({ case (dwcShortName, biocacheField) =>
        logger.debug(s"dwcShortName: $dwcShortName , biocacheField: $biocacheField")
      })
    }

    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis
    var currentBatch = new ArrayBuffer[FullRecord]

    val institutionCodes = Config.indexDAO.getDistinctValues("data_resource_uid:"+resourceUid, "institution_code", 100).getOrElse(List()).toSet[String]
    val collectionCodes = Config.indexDAO.getDistinctValues("data_resource_uid:"+resourceUid, "collection_code", 100).getOrElse(List()).toSet[String]

    logger.info("The current institution codes for the data resource: " + institutionCodes)
    logger.info("The current collection codes for the data resource: " + collectionCodes)

    val newCollCodes = new scala.collection.mutable.HashSet[String]
    val newInstCodes = new scala.collection.mutable.HashSet[String]

    while (iter.hasNext) {

      count += 1
      //the newly assigned record UUID
      val star = iter.next

      //the details of how to construct the UniqueID belong in the Collectory
      //val uniqueTermValues = uniqueTerms.map(t => dwc.getProperty(t))
      val uniqueID = {
        //create the unique ID
        if (!uniqueTerms.isEmpty) {
          val uniqueTermValues = uniqueTerms.map(t => star.core.value(t))
          val id = (List(resourceUid) ::: uniqueTermValues.toList).mkString("|").trim
          Some(if(stripSpaces) id.replaceAll("\\s","") else id)
        } else {
          None
        }
      }

      if(testFile){
        //check to see if the key has at least on distinguishing value
        val icode = star.core.value(org.gbif.dwc.terms.DwcTerm.institutionCode)
        newInstCodes.add(if(icode == null) "<NULL>" else icode)
        val ccode = star.core.value(org.gbif.dwc.terms.DwcTerm.collectionCode)
        newCollCodes.add(if(ccode == null) "<NULL>" else ccode)
      }

      //create a map of properties
      val fieldTuples = new ListBuffer[(String, String)]()
      fieldShortNameToIdxMap.foreach({ case (fieldIdx, modelProperty) =>
        val property = star.core.column(fieldIdx)
        if (logger.isDebugEnabled && StringUtils.isNotBlank(property)) {
          logger.debug(s"Mapped field: $modelProperty, fieldIdx: $fieldIdx, value: $property")
        }
        if (removeNullFields) {
          fieldTuples += (modelProperty -> property)
        } else {
          if (StringUtils.isNotBlank(property)) {
            fieldTuples += (modelProperty -> property)
          }
        }
      })

      if(logger.isDebugEnabled) {
        fieldTuples.foreach({case(key, value) => logger.debug(s"Not blank fields: $key , value: $value")})
      }

      //lookup the column
      val ((recordUuid, isNew), mappedProps) = getUuid(uniqueID, star, uniqueTerms, None)
      if(mappedProps.isDefined && uniqueID.isDefined){
        Config.persistenceManager.put(uniqueID.get, "occ", mappedProps.get, isNew, removeNullFields)
      }

      //add the record uuid to the map
      fieldTuples += ("uuid" -> recordUuid)
      //add the data resource uid
      fieldTuples += ("dataResourceUid" -> resourceUid)
      //add last load time
      fieldTuples += ("lastModifiedTime" -> loadTime)
      if(isNew){
        fieldTuples += ("firstLoaded" -> loadTime)
        newCount +=1
      }

      // Get any related multimedia
      val multimedia = loadMultimedia(star, DwCALoader.IMAGE_TYPE, imageBase) ++
        loadMultimedia(star, DwCALoader.MULTIMEDIA_TYPE, imageBase)

      // If there are no unique terms, use the UUID as a key
      // This isnt ideal and will stop any reloading
      val rowKey = if(uniqueID.isEmpty) {
        logger.warn("Unable to construct a unique key for this data resource. No unique terms defined.")
        resourceUid + "|" + recordUuid
      } else {
        uniqueID.get
      }

      if(rowKeyWriter.isDefined) {
        rowKeyWriter.get.write(rowKey + "\n")
      }

      if(!testFile){
        val fullRecord = FullRecordMapper.createFullRecord(rowKey, fieldTuples.toArray, Raw)
        processMedia(resourceUid, fullRecord, multimedia)
        currentBatch += fullRecord
      }

      //debug
      if (count % 1000 == 0 && count > 0) {
        if(!testFile){
          Config.occurrenceDAO.addRawOccurrenceBatch(currentBatch.toArray, removeNullFields)
        }
        finishTime = System.currentTimeMillis
        val timeInSecs = 1000 / (((finishTime - startTime).toFloat) / 1000f)
        logger.info(s"$count, >> last key : $uniqueID, UUID: $recordUuid, records per sec: $timeInSecs")
        startTime = System.currentTimeMillis
        //clear the buffer
        currentBatch.clear
      }
    }

    if(rowKeyWriter.isDefined){
      rowKeyWriter.get.flush
      rowKeyWriter.get.close
    }

    //check to see if the inst/coll codes are new
    if(testFile){

      val unknownInstitutions = newInstCodes &~ institutionCodes
      val unknownCollections = newCollCodes &~ collectionCodes

      if(!unknownInstitutions.isEmpty) {
        logger.warn("Warning there are new institution codes in the set: " + unknownInstitutions.mkString(","))
      }
      if(!unknownCollections.isEmpty) {
        logger.warn("Warning there are new collection codes in the set: " + unknownCollections.mkString(","))
      }

      //Report the number of new/existing records
      logger.info("There are " + count + " records in the file. The number of NEW records: " + newCount)
    }

    //commit the batch
    Config.occurrenceDAO.addRawOccurrenceBatch(currentBatch.toArray, removeNullFields)
    logger.info("Finished DwCA loader. Records loaded into the system: " + count)
    count
  }

  /**
   * Load multimedia via the supplied extension (rowType).
   *
   * @param star
   * @param rowType
   * @param imageBase
   * @return
   */
  def loadMultimedia(star: StarRecord, rowType: Term, imageBase: URL): Seq[Multimedia] = {
    if (!star.hasExtension(rowType)) {
      return List.empty
    }
    val records = star.extension(rowType).asScala
    val multimedia = new ListBuffer[Multimedia]
    records.foreach { row =>
      val terms = row.terms.filter { term => Option(row.value(term)).isDefined}
      val metadata = terms.map { term => term -> row.value(term) }.toMap[Term, String]
      locateMultimedia(row, imageBase) match {
        case Some(location) => multimedia.add(Multimedia.create(location, metadata))
        case None => logger.info("No location found for multimedia typed row: " + row)
      }
    }
    multimedia
  }

  def locateMultimedia(row: Record, imageBase: URL): Option[URL] = {
    val identifier = row.value(DcTerm.identifier)
    if(identifier != null){
      Some(new URL(imageBase, identifier))
    } else {
      None
    }
  }
}
