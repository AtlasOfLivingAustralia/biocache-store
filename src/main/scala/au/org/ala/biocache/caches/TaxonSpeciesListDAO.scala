package au.org.ala.biocache.caches

import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import au.org.ala.biocache.Config
import scala.collection.mutable
import scala.io.Source
import scala.util.parsing.json.JSON

/**
 * A cache that stores the species lists for a taxon lsid.
 *
 * It will only store the lists that are configured via the species lists property.
 */
object TaxonSpeciesListDAO {

  val logger = LoggerFactory.getLogger("TaxonSpeciesListDAO")

  private val lock : AnyRef = new Object()

  private var speciesListMap:Map[String, Seq[String]] = _


  def main(args:Array[String]): Unit = {

    val map = TaxonSpeciesListDAO.buildTaxonListMap

    println("Number of species in lists: " + map.keys.size)

    val counts = new mutable.HashMap[String, Int]

    map.keys.foreach { key =>
      val specieslistIds = map.get(key).get
      println(key + " - " + specieslistIds)
    }
  }

  /**
   * Get the list details for the supplied guid
   * @param conceptLsid
   * @return
   */
  def getCachedListsForTaxon(conceptLsid:String) : Seq[String] = {

    if( StringUtils.isEmpty(Config.listToolUrl) ){
      return (List.empty)
    }

    //check to see if the species list map has been initialised
    if(speciesListMap == null){
      lock.synchronized {
        //only le the first thread actually perform the init by rechecking the null
        if(speciesListMap == null){
          if(StringUtils.isNotEmpty(Config.listToolUrl)){
            logger.info("Loading list information...")
            speciesListMap = buildTaxonListMap
            logger.info("Loading list information...completed.")
          } else {
            logger.info("List tool URL is not configured. Skipping list loading...")
          }
        }
      }
    }

    //now return the the values
    speciesListMap.getOrElse(conceptLsid, List())
  }

  /**
   * Build a map of taxonID -> [list UID].
   *
   * @return
   */
  def buildTaxonListMap : Map[String, Seq[String]] = {

    try {
      //build up a map of taxonID -> list of species lists...
      val guidMap = new mutable.HashMap[String, Seq[String]]()

      getListUids.foreach { drUid =>

        logger.info(s"Loading data from $drUid")

        //csv download
        val downloadUrl = Config.listToolUrl + s"/speciesListItem/downloadList/$drUid?fetch=%7BkvpValues%3Dselect%7D"

        logger.info(s"Downloading list data from $downloadUrl")

        val data = scala.io.Source.fromURL(downloadUrl, "UTF-8").mkString

        //csv reader
        val csvReader = new CSVReader(new StringReader(data))
        val columnHdrs = csvReader.readNext()

        val guidIdx = columnHdrs.indexOf("guid")
        var currentLine = csvReader.readNext()

        //build up the map
        while (currentLine != null) {
          val guid = currentLine(guidIdx)
          val specieslists = guidMap.getOrElse(guid, List[String]())
          if (specieslists.isEmpty) {
            guidMap.put(guid, List(drUid))
          } else {
            guidMap.put(guid, specieslists :+ drUid)
          }

          currentLine = csvReader.readNext()
        }
      }

      guidMap.toMap
    } catch {
      case e:Exception => {
        logger.error("Problem loading lists to intersect with." + e.getMessage, e)
        Map()
      }
    }
  }

  /**
   * Retrieve a list of species lists UIDs.
   *
   * @return
   */
  private def getListUids : Seq[String] = {

    val listsUrl = Config.listToolUrl + "/ws/speciesList?isAuthoritative=eq:true"
    val jsonString = Source.fromURL(listsUrl, "UTF-8").mkString
    JSON.parseFull(jsonString) match {
      case Some(json) => {
        val map = json.asInstanceOf[Map[String, Any]]
        map.get("lists") match {
          case Some(lists) => {
            lists.asInstanceOf[Seq[Map[String, Any]]].map { listDetails =>
              listDetails.getOrElse("dataResourceUid", "").toString
            }.toList
          }
          case None => {
            logger.warn("Unable to load ids of authoritative lists")
            List()
          }
        }
      }
      case None => {
        logger.warn("Unable to load ids of authoritative lists")
        List()
      }
    }
  }

//
//  /**
//   * Retrieve a list of data resource UIDs for the supplied query.
//   *
//   * @param listToolQuery
//   * @return
//   */
//  private def getListsForQuery(listToolQuery:String) : Seq[String] = {
//
//    if( StringUtils.isEmpty(Config.listToolUrl) ){
//      return List.empty
//    }
//
//    val speciesLists = Json.toJavaMap(WebServiceLoader.getWSStringContent(Config.listToolUrl + "/speciesList?" + listToolQuery))
//    var ids = List[String]()
//    if (speciesLists.containsKey("lists")) {
//      val authlists = speciesLists.get("lists").asInstanceOf[util.List[util.Map[String, Object]]]
//      for (listIdx <- 0 until authlists.size()) {
//        if (authlists.get(listIdx).containsKey("dataResourceUid")) {
//          ids = authlists.get(listIdx).get("dataResourceUid").toString :: ids
//        }
//      }
//    }
//    ids
//  }
//
//  /**
//   * Updates the lists as configured to indicate which lists and properties are applicable for each guid.
//   *
//   * @return
//   */
//  private def updateLists : Map[String,(List[String], Map[String,String])] = {
//
//    if( StringUtils.isEmpty(Config.listToolUrl) ){
//      return Map.empty
//    }
//
//    logger.info("Updating the lists")
//    val newMap = new scala.collection.mutable.HashMap[String, (List[String], Map[String,String])]()
//    val guidsArray = new ArrayBuffer[String]()
//
//    validLists.foreach { v =>
//      //get the guids on the list
//      val url = MessageFormat.format(guidUrl, v)
//      val response = WebServiceLoader.getWSStringContent(url)
//      if(response != ""){
//        val list = JSON.parseFull(response).get.asInstanceOf[List[String]]
//        guidsArray ++= list.filter(_ != null)
//      }
//    }
//
//    val guids = guidsArray.toSet
//    //now load all the details
//    logger.info("The number of distinct species " + guids.size)
//    var counter = 0
//    guids.foreach(guid => {
//      counter += 1
//      if(counter % 100 == 0){
//        logger.info(s"$counter species load")
//      }
//      //get the values from the cache
//      newMap(guid) = getListsForTaxon(guid, true)
//    })
//    logger.info("Finished creating the map")
//    newMap.toMap
//  }
//
//  /**
//   * Retrieves the species list information from the WS
//   */
//  private def getListsForTaxon(conceptLsid:String, forceLoad:Boolean):(List[String], Map[String,String])={
//
//    val response = WebServiceLoader.getWSStringContent(listToolUrl + conceptLsid)
//
//    if(response != ""){
//
//      val list = JSON.parseFull(response).get.asInstanceOf[List[Map[String, AnyRef]]]
//
//      //get a distinct list of lists that the supplied concept belongs to
//      val newObject = list.map(_.getOrElse("dataResourceUid","").toString).filter(v => validLists.contains(v)).toSet.toList
//      val newMap = collection.mutable.Map[String, String]()
//      list.foreach(map => {
//        val dr = map.getOrElse("dataResourceUid","").toString
//        //only add the KVP items if they come from valid list
//        if(validLists.contains(dr)){
//
//          val kvpArrayOpt = map.get("kvpValues")
//
//          if(kvpArrayOpt.isDefined){
//            val kvpArray = kvpArrayOpt.get.asInstanceOf[List[Map[String,String]]]
//            //get the prefix information
//            val prefix = kvpArray.find(map => Config.stateProvincePrefixFields.contains(map.getOrElse("key","")))
//            val stringPrefix = {
//              if(prefix.isDefined){
//                val value = getValueBasedOnKVP(prefix.get)
//                val matchedPrefix = SpeciesListAcronyms.matchTerm(value)
//                if(matchedPrefix.isDefined) {
//                  matchedPrefix.get.canonical.toLowerCase()
//                } else {
//                  value.replaceAll(" " , "_").toLowerCase()
//                }
//              } else {
//                ""
//              }
//            }
//
//            val filteredList = kvpArray.filter(_.values.toSet.intersect(Config.speciesListIndexValues).size > 0)
//
//            filteredList.foreach( item => {
//              //now grab the indexItems
//              if(Config.speciesListIndexValues.contains(item.getOrElse("key",""))){
//                val value = getValueBasedOnKVP(item)
//                newMap(dr + getEscapedValue(stringPrefix) + getEscapedValue(item.getOrElse("key",""))) = value
//              }
//            })
//          }
//        }
//      })
//      lock.synchronized { lru.put(conceptLsid, (newObject, newMap.toMap)) }
//      (newObject, newMap.toMap)
//    } else {
//      (List(), Map())
//    }
//  }
//
//  private def getEscapedValue(value:String) = if(value.trim.size > 0){
//    "_" + value
//  } else {
//    value.trim
//  }
//
//  private def getValueBasedOnKVP(item:Map[String,String]):String = {
//    if (item.getOrElse("vocabValue",null) != null) {
//      item.getOrElse("vocabValue",null)
//    } else {
//      item.getOrElse("value","")
//    }
//  }
}
