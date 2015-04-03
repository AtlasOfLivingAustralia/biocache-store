package au.org.ala.biocache.caches

import java.util

import org.slf4j.LoggerFactory
import au.org.ala.biocache.Config
import scala.collection.mutable.ArrayBuffer
import java.text.MessageFormat
import scala.util.parsing.json.JSON
import au.org.ala.biocache.persistence.PersistenceManager
import au.org.ala.biocache.util.Json
import au.org.ala.biocache.vocab.SpeciesListAcronyms

/**
 * A cache that stores the species lists for a taxon lsid.
 *
 * It will only store the lists that are configured via the species lists property.
 */
object TaxonSpeciesListDAO {

  val logger = LoggerFactory.getLogger("TaxonSpeciesListDAO")

  def listToolUrl = Config.listToolUrl + "/species/"

  val guidUrl = Config.listToolUrl + "/speciesList/{0}/taxa"

  private val lru = new org.apache.commons.collections.map.LRUMap(100000)
  private val lock : AnyRef = new Object()

  private var speciesListMap:Map[String,(List[String], Map[String,String])] = _

  private val validLists = getListsForQuery("isAuthoritative=eq:true")

  def getListsForQuery(listToolQuery:String) : Seq[String] = {
    val speciesLists = Json.toJavaMap(WebServiceLoader.getWSStringContent(Config.listToolUrl + "/speciesList?" + listToolQuery))
    var ids = List[String]()
    if (speciesLists.containsKey("lists")) {
      val authlists = speciesLists.get("lists").asInstanceOf[util.List[util.Map[String, Object]]]
      for (listIdx <- 0 until authlists.size()) {
        if (authlists.get(listIdx).containsKey("dataResourceUid")) {
          ids = authlists.get(listIdx).get("dataResourceUid").toString :: ids
        }
      }
    }
    ids
  }

  /**
   * Updates the lists as configured to indicate which lists and properties are applicable for each guid
   * @return
   */
  private def updateLists : Map[String,(List[String], Map[String,String])] = {
    logger.info("Updating the lists")
    val newMap = new scala.collection.mutable.HashMap[String, (List[String], Map[String,String])]()
    val guidsArray = new ArrayBuffer[String]()

    validLists.foreach(v => {
      //get the guids on the list
      val url = MessageFormat.format(guidUrl, v)
      val response = WebServiceLoader.getWSStringContent(url)
      if(response != ""){
        val list = JSON.parseFull(response).get.asInstanceOf[List[String]]
        guidsArray ++= list.filter(_ != null)
      }
    })
    val guids = guidsArray.toSet
    //now load all the details
    logger.info("The number of distinct species " + guids.size)
    guids.foreach(guid => {
      //get the values from the cache
      newMap(guid) = TaxonSpeciesListDAO.getListsForTaxon(guid, true)
    })
    logger.info("Finished creating the map")
    newMap.toMap
  }

  /**
   * Get the list details for the supplied guid
   * @param conceptLsid
   * @return
   */
  def getCachedListsForTaxon(conceptLsid:String):(List[String], Map[String,String]) = {
    //check to see if the species list map has been initialised
    if(speciesListMap == null){
      lock.synchronized {
        //only le the first thread actually perform the init by rechecking the null
        if(speciesListMap == null){
          speciesListMap = updateLists
        }
      }
    }
    //now return the the values
    speciesListMap.getOrElse(conceptLsid, (List(), Map()))
  }

  /**
   * Retrieves the species list information from the WS
   */
  def getListsForTaxon(conceptLsid:String, forceLoad:Boolean):(List[String], Map[String,String])={

    val response = WebServiceLoader.getWSStringContent(listToolUrl + conceptLsid)

    if(response != ""){

      val list = JSON.parseFull(response).get.asInstanceOf[List[Map[String, AnyRef]]]

      //get a distinct list of lists that the supplied concept belongs to
      val newObject = list.map(_.getOrElse("dataResourceUid","").toString).filter(v => validLists.contains(v)).toSet.toList
      val newMap = collection.mutable.Map[String, String]()
      list.foreach(map => {
        val dr = map.getOrElse("dataResourceUid","").toString
        //only add the KVP items if they come from valid list
        if(validLists.contains(dr)){

          val kvpArrayOpt = map.get("kvpValues")

          if(kvpArrayOpt.isDefined){
            val kvpArray = kvpArrayOpt.get.asInstanceOf[List[Map[String,String]]]
            //get the prefix information
            val prefix = kvpArray.find(map => Config.stateProvincePrefixFields.contains(map.getOrElse("key","")))
            val stringPrefix = {
              if(prefix.isDefined){
                val value = getValueBasedOnKVP(prefix.get)
                val matchedPrefix = SpeciesListAcronyms.matchTerm(value)
                if(matchedPrefix.isDefined) {
                  matchedPrefix.get.canonical.toLowerCase()
                } else {
                  value.replaceAll(" " , "_").toLowerCase()
                }
              } else {
                ""
              }
            }

            val filteredList = kvpArray.filter(_.values.toSet.intersect(Config.speciesListIndexValues).size > 0)

            filteredList.foreach( item => {
              //now grab the indexItems
              if(Config.speciesListIndexValues.contains(item.getOrElse("key",""))){
                val value = getValueBasedOnKVP(item)
                newMap(dr + getEscapedValue(stringPrefix) + getEscapedValue(item.getOrElse("key",""))) = value
              }
            })
          }
        }
      })
      lock.synchronized { lru.put(conceptLsid, (newObject, newMap.toMap)) }
      (newObject, newMap.toMap)
    } else {
      (List(), Map())
    }
  }

  private def getEscapedValue(value:String) = if(value.trim.size > 0){
    "_" + value
  } else {
    value.trim
  }

  private def getValueBasedOnKVP(item:Map[String,String]):String = {
    if (item.getOrElse("vocabValue",null) != null) {
      item.getOrElse("vocabValue",null)
    } else {
      item.getOrElse("value","")
    }
  }
}
