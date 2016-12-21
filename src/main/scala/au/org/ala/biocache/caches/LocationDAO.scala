package au.org.ala.biocache.caches

import org.slf4j.LoggerFactory
import au.org.ala.biocache.Config
import scala.collection.mutable.HashMap
import au.org.ala.biocache.model.Location

/**
 * DAO for location lookups (lat, long -> locality).
 */
object LocationDAO {

  val logger = LoggerFactory.getLogger("LocationDAO")

  private val columnFamily = "loc"
  private val lock : AnyRef = new Object()
  private val lru = new org.apache.commons.collections.map.LRUMap(10000)

  private final val latitudeCol = "lat"
  private final val longitudeCol = "lon"

  /**
   * Add a region mapping for this point.
   *
   * For return values when batch == true commit with writeLocBatch
   */
  def addLayerIntersects (latitude:String, longitude:String, contextual:Map[String,String], environmental:Map[String,Float], batch:Boolean = false) : (String, Map[String, String]) = {
    if (latitude != null && latitude.trim.length > 0 && longitude != null && longitude.trim.length > 0){

      val guid = getLatLongKey(latitude, longitude)

      val mapBuffer = new HashMap[String, String]

      mapBuffer += (latitudeCol -> latitude)
      mapBuffer += (longitudeCol-> longitude)
      mapBuffer ++= contextual
      mapBuffer ++= environmental.map(x => x._1 -> x._2.toString)

      if (batch) {
        (guid -> mapBuffer.toMap)
      } else {
        Config.persistenceManager.put(guid, columnFamily, mapBuffer.toMap, false)
        null
      }
    } else {
      null
    }
  }

  /**
   * Write a list of maps produced by addLayerIntersects with batch == true
   *
   * @param batch
   */
  def writeLocBatch(batch: collection.Map[String, Map[String, String]]) {
    var retries = 0
    var processedOK = false
    while (!processedOK && retries < 6) {
      try {
        Config.persistenceManager.putBatch(columnFamily, batch.toMap, false)
        processedOK = true
      } catch {
        case e: Exception => {
          logger.error("Error processing record batch with length: '" + batch.size + "',  sleeping for 20 secs before retries", e)
          Thread.sleep(20000)
          retries += 1
        }
      }
    }
  }

  def getLatLongKey(latitude:String, longitude:String) : String = {
    latitude.toFloat.toString.trim + "|" + longitude.toFloat.toString
  }

  private def getLatLongKey(latitude:Float, longitude:Float) : String = {
    latitude.toString.trim + "|" + longitude.toString
  }

  private def getLatLongKey(latitude:Double, longitude:Double) : String = {
    latitude.toString.trim + "|" + longitude.toString
  }

  def storePointForSampling(latitude:String, longitude:String) : String = {
    val uuid = getLatLongKey(latitude, longitude)
    val map = Map(latitudeCol -> latitude, longitudeCol -> longitude)
    Config.persistenceManager.put(uuid, columnFamily, map, false)
    uuid
  }

  /**
   * Get location information for point.
   * For geo spatial requirements we don't want to round the latitude , longitudes
   */
  def getSamplesForLatLon(latitude:String, longitude:String) : Option[(Location, collection.Map[String,String], collection.Map[String,String])] = {

    if (latitude == null || longitude == null || latitude.trim.length == 0 || longitude.trim.length == 0){
      return None
    }

    val uuid = getLatLongKey(latitude, longitude)

    val cachedObject = lock.synchronized { lru.get(uuid) }

    if(cachedObject != null){

      cachedObject.asInstanceOf[Option[(Location, Map[String, String], Map[String, String])]]

    } else {

      val map = Config.persistenceManager.get(uuid, columnFamily)
      map match {
        case Some(map) => {
          val location = new Location
          location.decimalLatitude = latitude
          location.decimalLongitude = longitude

          val el = map.filter(x => x._1.startsWith("el"))
          val cl = map.filter(x => x._1.startsWith("cl"))

          val returnValue = Some((location, el, cl))

          lock.synchronized { lru.put(uuid, returnValue) }

          returnValue
        }
        case None => {
          if(!Config.fieldsToSample(false).isEmpty) {
            logger.warn("Location lookup failed for [" + latitude + "," + longitude + "] - Sampling may need to be re-ran")
          }
          None
        }
      }
    }
  }
}
