package au.org.ala.biocache.load

import java.net.URL
import java.util
import java.util.Date

import au.org.ala.biocache._
import java.io.{BufferedOutputStream, FileOutputStream, File}
import au.org.ala.biocache.cmd.NoArgsTool
import au.org.ala.biocache.util.Json
import org.gbif.dwc.terms.{DwcTerm, GbifTerm}
import org.gbif.dwc.text.{Archive, ArchiveFactory}
import org.slf4j.LoggerFactory

import scala.collection.mutable.{ListBuffer, ArrayBuffer}
import java.text.MessageFormat
import au.org.ala.names.model.LinnaeanRankClassification
import au.org.ala.biocache.caches.{WebServiceLoader, TaxonSpeciesListDAO}
import au.org.ala.biocache.model.{ConservationStatus}
import scala.util.parsing.json.JSON
import sys.process._
/**
 * A loader that imports data from IRMNG exports.
 *
 * Download DwC archive from:
 * http://www.cmar.csiro.au/datacentre/downloads/IRMNG_DWC.zip
 */
object HabitatLoader extends NoArgsTool {

  val logger = LoggerFactory.getLogger("HabitatLoader")
  def cmd = "update-habitat-data"
  def desc = "Load habitat data from sources (e.g. IRMNG)"

  def main(args: Array[String]): Unit = {
    proceed(args, () => run())
  }

  def run() {
    import scala.collection.JavaConverters._

    var counter = 0

    val archiveFile = Config.tmpWorkDir + File.separator + "IRMNG_DWC.zip"

    //download the archive
    logger.info("Downloading the IRMNG archive...")
    new URL(Config.irmngDwcArchiveUrl) #> new File(Config.tmpWorkDir +  File.separator +  "IRMNG_DWC.zip") !!

    val myArchiveFile = new File(archiveFile)
    val extractToFolder = new File(Config.tmpWorkDir + File.separator + "IRMNG_DWC")
    val dwcArchive:Archive = ArchiveFactory.openArchive(myArchiveFile, extractToFolder)
    logger.info("Archive rowtype: " + dwcArchive.getCore().getRowType() + ", "
      + dwcArchive.getExtensions().size() + " extension(s)")

    dwcArchive.asScala.foreach { starRecord =>

      val cl = new LinnaeanRankClassification()
      val scientificName = starRecord.core().value(DwcTerm.scientificName)
      cl.setScientificName(scientificName)
      cl.setSpecificEpithet(starRecord.core().value(DwcTerm.specificEpithet))
      cl.setGenus(starRecord.core().value(DwcTerm.genus))
      cl.setFamily(starRecord.core().value(DwcTerm.family))

      val guid = {
        try {
          Config.nameIndex.searchForAcceptedLsidDefaultHandling(cl, false)
        } catch {
          case e:Exception => {
            println("Problem looking up name: " + scientificName+ ". " + e.getMessage)
            null
          }
        }
      }

      if(guid != null){
        if (starRecord.extensions().containsKey(GbifTerm.SpeciesProfile)) {
          starRecord.extension(GbifTerm.SpeciesProfile).asScala.foreach { extRec =>
            val rawValue = extRec.value(GbifTerm.isMarine)
            if(rawValue !=null && rawValue != "null"){
              val habitat = if(rawValue.toBoolean){
                "Marine"
              } else {
                "Terrestrial"
              }
              Config.persistenceManager.put(guid, "taxon", Map("habitats" -> habitat))
              counter += 1
              if(counter % 1000 == 0) {
                println(s"Habitat values loaded: $counter")
              }
            }
          }
        }
      }
    }
    println(s"Finished. Total habitat values loaded: $counter")
  }
}

/**
 * Loads the taxon profile information from the species list tool.
 */
object ConservationListLoader extends NoArgsTool {

  val logger = LoggerFactory.getLogger("ConservationListLoader")
  def cmd = "update-conservation-data"
  def desc = "Load conservation data from sources (e.g. list tool)"

  val guidUrl = Config.listToolUrl + "/speciesList/{0}/taxa"
  val guidsArray = new ArrayBuffer[String]()

  def getListsForQuery(listToolQuery:String) : Seq[(String, String)] = {
    val speciesLists = Json.toJavaMap(WebServiceLoader.getWSStringContent(Config.listToolUrl + "/speciesList?" + listToolQuery))
    val ids = ListBuffer[(String, String)]()
    if (speciesLists.containsKey("lists")) {
      val authLists = speciesLists.get("lists").asInstanceOf[util.List[util.Map[String, Object]]]
      for (listIdx <- 0 until authLists.size()) {

        val listProperties = authLists.get(listIdx)

        if (listProperties.containsKey("dataResourceUid") && listProperties.get("region") != null) {
          ids +=( (listProperties.get("dataResourceUid").toString, listProperties.get("region").toString) )
        }
      }
    }
    ids
  }

  def main(args: Array[String]) {
    proceed(args, () => run())
  }

  def run() {

    val listUids = getListsForQuery("isThreatened=eq:true")

    // grab a list of distinct guids that form the list
    listUids.foreach { case (listUid, region) => {
      //get the taxon guids on the list
      val url = MessageFormat.format(guidUrl, listUid)
      val response = WebServiceLoader.getWSStringContent(url)
      if(response != ""){
        val list = JSON.parseFull(response).get.asInstanceOf[List[String]]
        guidsArray ++= list.filter(_ != null)
      }
    }}
    val guids = guidsArray.toSet

    //now load all the details for each  taxon guids
    logger.info("The number of distinct species " + guids.size)
    var counter = 0
    guids.foreach(guid => {

      counter += 1
      if(counter % 100 == 0){
        logger.info(s"$counter species load")
      }
      //get the values from the cache
//      val (lists, props) = TaxonSpeciesListDAO.getCachedListsForTaxon(guid)
      //now add the values to the DB
      val buff = new ListBuffer[ConservationStatus]
//
//      listUids.foreach { case (listUid, region) => {
//        if(props.getOrElse(listUid + "_status", "") != ""){
//          val status = props.getOrElse(listUid + "_status", "")
//          val rawStatus = props.getOrElse(listUid + "_sourceStatus", "")
//          val conservationStatus = new ConservationStatus(
//            region,
//            "",
//            status,
//            rawStatus
//          )
//          println(guid + ": " + conservationStatus)
//          buff += conservationStatus
//        }
//      }}

      if(!buff.isEmpty) {
        val csAsJson = Json.toJSON(buff.toList)
        Config.persistenceManager.put(guid, "taxon", Map("conservation" -> csAsJson))
      }
    })
  }
}