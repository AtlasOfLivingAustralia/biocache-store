package au.org.ala.biocache.load

import java.util

import au.org.ala.biocache._
import au.com.bytecode.opencsv.CSVReader
import java.io.FileReader
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.util.Json
import org.slf4j.LoggerFactory

import scala.collection.mutable.{ListBuffer, ArrayBuffer}
import java.text.MessageFormat
import au.org.ala.names.model.LinnaeanRankClassification
import au.org.ala.biocache.caches.{WebServiceLoader, TaxonSpeciesListDAO, TaxonProfileDAO}
import au.org.ala.biocache.model.{ConservationStatus, TaxonProfile}
import scala.util.parsing.json.JSON

/**
 * A loader that imports data from IRMNG exports.
 *
 * TODO use the GBIF DwC archive of IRMNG to load this information
 *
 * //download DwC archive
 * //http://www.cmar.csiro.au/datacentre/downloads/IRMNG_DWC.zip
 */
object HabitatLoader {

  def main(args:Array[String]){

    //read with DwC reader
    //get the habitat information
    val files = if(args.size == 0) {
      Array(
        "/data/biocache-load/species_list.txt",
        "/data/biocache-load/family_list.txt",
        "/data/biocache-load/genus_list.txt")
    } else {
      args
    }
    files.foreach { file => processFile(file) }
  }

  def processFile(file:String){

    println("Loading habitats from " + file)
    val reader =  new CSVReader(new FileReader(file), '\t', '"', '~')
    var currentLine = reader.readNext()
    var previousScientificName = ""
    var count = 0
    var loaded = 0
    while(currentLine != null){
      val currentScientificName = currentLine(1)
      var habitat:String=null
      if(currentScientificName != null && !currentScientificName.equalsIgnoreCase(previousScientificName)){
        val cl = new LinnaeanRankClassification()
        cl.setScientificName(currentScientificName)
        if (currentLine.length == 12){
          //we are dealing with a family
          cl.setFamily(currentScientificName)
          cl.setKingdom(currentLine(2))
          habitat = getValue(currentLine(4))
        } else if (currentLine.length==13){
          //dealing with genus or species
          val isGenus = !currentScientificName.contains(" ")
          if(isGenus){
            cl.setGenus(currentLine(1))
            if(currentLine(2).contains("unallocated")){
              cl.setFamily(currentLine(2))
            }
          } else {
            cl.setGenus(currentLine(2))
          }
        }
//        val guid = Config.nameIndex.searchForAcceptedLsidDefaultHandling(cl,false)
        try {
          val guid = Config.nameIndex.searchForLSID(currentScientificName)
          previousScientificName = currentScientificName
          if (guid != null && habitat != null) {
            //add the habitat status
            Config.persistenceManager.put(guid, "taxon", "habitats", habitat)
            loaded += 1
            //println("Adding " +habitat + " for " +guid)
          }
        } catch {
          case e:Exception => println("Error loading: " + currentScientificName + " - " + e.getMessage)
        }
        count += 1
        if(count % 10000 == 0){
          println(s"Processed $count, loaded $loaded taxon >>>> " + currentLine.mkString(","))
        }
      }
      currentLine = reader.readNext()
    }
  }

  def getValue(v:String) : String = {
    v match {
      case it if it == 'M' => "Marine"
      case it if it == "N" => "Non-Marine"
      case it if it == "MN" => "Marine and Non-marine"
      case _ => null
    }
  }
}

/**
* Loads the taxon profile information from the species list tool.
*/
object ConservationListLoader extends Tool {

  val logger = LoggerFactory.getLogger("ConservationListLoader")
  def cmd = "update-conservation-data"
  def desc = "Load conservation data from sources (e.g. list tool)"

  val guidUrl = Config.listToolUrl + "/speciesList/{0}/taxa"
  val guidsArray = new ArrayBuffer[String]()

  def getListsForQuery(listToolQuery:String) : Seq[(String, String)] = {
    val speciesLists = Json.toJavaMap(WebServiceLoader.getWSStringContent(Config.listToolUrl + "/speciesList?" + listToolQuery))
    val ids = ListBuffer[(String, String)]()
    if (speciesLists.containsKey("lists")) {
      val authlists = speciesLists.get("lists").asInstanceOf[util.List[util.Map[String, Object]]]
      for (listIdx <- 0 until authlists.size()) {

        val listProperties = authlists.get(listIdx)

        if (listProperties.containsKey("dataResourceUid") && listProperties.get("region") != null) {
          ids +=( (listProperties.get("dataResourceUid").toString, listProperties.get("region").toString) )
        }
      }
    }
    ids
  }

  def main(args:Array[String]){

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
    guids.foreach(guid => {
      //get the values from the cache
      val (lists, props) = TaxonSpeciesListDAO.getCachedListsForTaxon(guid)
      //now add the values to the DB
      val buff = new ListBuffer[ConservationStatus]

      listUids.foreach { case (listUid, region) => {
        if(props.getOrElse(listUid + "_status", "") != ""){
          val status = props.getOrElse(listUid + "_status", "")
          val rawStatus = props.getOrElse(listUid + "_sourceStatus", "")

          val conservationStatus = new ConservationStatus(
            region,
            "",
            status,
            rawStatus
          )
          println(guid + ": " + conservationStatus)
          buff += conservationStatus
        }
      }}

      val csAsJson = Json.toJSON(buff.toList)

      Config.persistenceManager.put(guid, "taxon", Map("conservation" -> csAsJson))
    })
  }
}