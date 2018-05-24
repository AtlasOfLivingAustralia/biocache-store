package au.org.ala.biocache.caches

import au.org.ala.biocache._
import au.org.ala.biocache.caches.ClassificationDAO.lru

import scala.Some
import au.org.ala.biocache.model.{ConservationStatus, SensitiveSpecies, TaxonProfile}
import au.org.ala.biocache.util.Json

/**
 * A DAO for accessing taxon profile information by GUID.
 *
 * This should provide an abstraction layer, that (eventually) handles
 * "timeToLive" style functionality that invalidates values in the cache
 * and retrieves the latest values.
 */
object TaxonProfileDAO {

  private val columnFamily = "taxon"
  private val lru: java.util.Map[String, Option[TaxonProfile]] = {
    if (Config.taxonProfileCacheAll) {
      new java.util.HashMap[String, Option[TaxonProfile]]()
    } else {
      new org.apache.commons.collections.map.LRUMap(Config.taxonProfileCacheSize)
    }
  }.asInstanceOf[java.util.Map[String, Option[TaxonProfile]]]
  private val lock: AnyRef = new Object()
  private val persistenceManager = Config.persistenceManager

  /**
   * Retrieve the profile by the taxon concept's GUID
   */
  def createTaxonProfile(map: Option[Map[String, String]]): TaxonProfile = {
      val taxonProfile = new TaxonProfile
      map.get.foreach(keyValue => {
        keyValue._1 match {
          case "guid" => taxonProfile.guid = keyValue._2
          case "commonName" => taxonProfile.commonName = keyValue._2
          case "habitats" => if (keyValue._2 != null && !keyValue._2.isEmpty) {
            taxonProfile.habitats = keyValue._2.split(",")
          }
          case "conservation" => if(keyValue._2 != null && keyValue._2.size > 0){
            taxonProfile.conservation = Json.toArray(keyValue._2, classOf[ConservationStatus].asInstanceOf[java.lang.Class[AnyRef]]).asInstanceOf[Array[ConservationStatus]]
          }
          case _ => //ignore
        }
      })
      taxonProfile
  }

  def getByGuid(guid:String) : Option[TaxonProfile] = {

    if(!Config.taxonProfilesEnabled) return None

    if (Config.taxonProfileCacheAll) {
      lock.synchronized {
        if (lru.size() == 0) {
          persistenceManager.pageOverAll(columnFamily, (guid, map) => {
            if (!map.isEmpty) {
              val result = Some(createTaxonProfile(Some(map)))
              lru.put(guid, result)
            }

            true
          })
        }
      }
    }

    if(guid == null || guid.isEmpty) return None

    val taxonProfile = {

      val cachedObject = lock.synchronized { lru.get(guid) }
      if(cachedObject == null){
        val map = persistenceManager.get(guid,columnFamily)
        if(!map.isEmpty){
          val result = Some(createTaxonProfile(map))
          lock.synchronized { lru.put(guid, result) }
          result
        } else {
          lock.synchronized { lru.put(guid, None) }
          None
        }
      } else {
        cachedObject
      }
    }
    taxonProfile.asInstanceOf[Option[TaxonProfile]]
  }

  /**
   * Persist the taxon profile.
   */
  def add(taxonProfile:TaxonProfile) {

      val properties = scala.collection.mutable.Map[String,String]()
      properties.put("guid", taxonProfile.guid)
      properties.put("commonName", taxonProfile.commonName)
      if(taxonProfile.habitats!=null && !taxonProfile.habitats.isEmpty){
        val habitatString = taxonProfile.habitats.reduceLeft(_+","+_)
        properties.put("habitats", habitatString)
      }
      if(taxonProfile.conservation != null && !taxonProfile.conservation.isEmpty){
        properties.put("conservation", Json.toJSON(taxonProfile.conservation.asInstanceOf[Array[AnyRef]]))
      }
      persistenceManager.put(taxonProfile.guid, columnFamily, properties.toMap, true, false)

      if (Config.taxonProfileCacheAll) {
        lock.synchronized {
          lru.put(taxonProfile.guid, Some(taxonProfile))
        }
      }
  }

  def getCacheSize = lru.size()
}
