package au.org.ala.biocache.caches

import au.org.ala.biocache._
import scala.Some
import au.org.ala.biocache.model.{TaxonProfile, SensitiveSpecies, ConservationStatus}
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
  private val lru = new org.apache.commons.collections.map.LRUMap(10000)
  private val lock : AnyRef = new Object()
  private val persistenceManager = Config.persistenceManager

  /**
   * Retrieve the profile by the taxon concept's GUID
   */
  def createTaxonProfile(map: Option[Map[String, String]]): TaxonProfile = {
      val taxonProfile = new TaxonProfile
      map.get.foreach(keyValue => {
        keyValue._1 match {
          case "guid" => taxonProfile.guid = keyValue._2
          case "habitats" => if (keyValue._2 != null && !keyValue._2.isEmpty) {
            taxonProfile.habitats = keyValue._2.split(",")
          }
          case "conservation" => if(keyValue._2 != null && keyValue._2.size >0){
            taxonProfile.conservation = Json.toArray(keyValue._2, classOf[ConservationStatus].asInstanceOf[java.lang.Class[AnyRef]]).asInstanceOf[Array[ConservationStatus]]
          }
          case _ => //ignore
        }
      })
      taxonProfile
  }

  def getByGuid(guid:String) : Option[TaxonProfile] = {

    if(guid == null || guid.isEmpty) return None

    val taxonProfile = {

      val cachedObject = lock.synchronized { lru.get(guid) }
      if(cachedObject == null){
        val map = persistenceManager.get(guid,columnFamily)
        if(!map.isEmpty){
          val result = Some(createTaxonProfile(map))
          lock.synchronized { lru.put(guid,result) }
          result
        } else {
          lock.synchronized { lru.put(guid,None) }
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
      if(taxonProfile.habitats!=null && !taxonProfile.habitats.isEmpty){
        val habitatString = taxonProfile.habitats.reduceLeft(_+","+_)
        properties.put("habitats", habitatString)
      }
      if(taxonProfile.conservation != null && taxonProfile.conservation.size >0){
        properties.put("conservation", Json.toJSON(taxonProfile.conservation.asInstanceOf[Array[AnyRef]]))
      }
      persistenceManager.put(taxonProfile.guid, columnFamily, properties.toMap)
  }
}
