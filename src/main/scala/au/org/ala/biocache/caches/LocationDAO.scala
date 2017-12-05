package au.org.ala.biocache.caches

import java.io.FileWriter
import java.util.concurrent.ConcurrentHashMap

import au.org.ala.biocache.Config
import au.org.ala.biocache.model.Location
import org.slf4j.LoggerFactory

import scala.collection.mutable.HashMap
import scala.util.Try

/**
 * DAO for location lookups (lat, long -> locality).
 */
object LocationDAO {

  val logger = LoggerFactory.getLogger("LocationDAO")

  private val columnFamily = "loc"
  private val lock : AnyRef = new Object()
  private val lru = new org.apache.commons.collections.map.LRUMap(10000)
  private val storedPointCache = ConcurrentHashMap.newKeySet[String]()


  private final val latitudeCol = "lat"
  private final val longitudeCol = "lon"

  var persistPointsFile:FileWriter = null

  /**
   * Add a region mapping for this point.
   *
   * For return values when batch == true commit with writeLocBatch
   */
  def addLayerIntersects (latitude:String, longitude:String, contextual:String, environmental:String, batch:Boolean = false) : (String, Map[String, String]) = {
    if (latitude != null && latitude.trim.length > 0 && longitude != null && longitude.trim.length > 0){

      val guid = getLatLongKey(latitude, longitude)

      if (guid.length > 0) {
        val mapBuffer = new HashMap[String, String]

        mapBuffer += (latitudeCol -> latitude)
        mapBuffer += (longitudeCol -> longitude)
        mapBuffer += ("cl" -> contextual)
        mapBuffer += ("el" -> environmental)

        if (batch) {
          (guid -> mapBuffer.toMap)
        } else {
          Config.persistenceManager.put(guid, columnFamily, mapBuffer.toMap, true, false)
          null
        }
      } else {
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
      Config.persistenceManager.putBatch(columnFamily, batch.toMap, true, false)
  }

  def getLatLongKey(latitude:String, longitude:String) : String = {
    Try { latitude.toFloat.toString.trim + "|" + longitude.toFloat.toString }.getOrElse("")
  }

  private def getLatLongKey(latitude:Float, longitude:Float) : String = {
    latitude.toString.trim + "|" + longitude.toString
  }

  private def getLatLongKey(latitude:Double, longitude:Double) : String = {
    latitude.toString.trim + "|" + longitude.toString
  }

  def storePointForSampling(latitude:String, longitude:String) : String = {
    val uuid = getLatLongKey(latitude, longitude)
    if(storedPointCache.contains(uuid)){
      return uuid
    }
    if (uuid.length > 0) {
      val map = Map(latitudeCol -> latitude, longitudeCol -> longitude)
      if (Config.persistPointsFile != "") {
        synchronized {
          if (persistPointsFile == null) {
            persistPointsFile = new FileWriter(Config.persistPointsFile)
          }
          persistPointsFile.write(longitude + "," + latitude + "\n")
          persistPointsFile.flush()
        }
      } else {
        Config.persistenceManager.put(uuid, columnFamily, map, true, false)
        storedPointCache.add(uuid)
      }
    }
    uuid
  }

  def closePointsFile(): Unit ={
    if(persistPointsFile != null){
      persistPointsFile.flush()
      persistPointsFile.close()
    }
  }

  /**
   * Get location information for point.
   * For geo spatial requirements we don't want to round the latitude , longitudes
   */
  def getSamplesForLatLon(latitude:String, longitude:String) : Option[(Location, String, String)] = {

    if (latitude == null || longitude == null || latitude.trim.length == 0 || longitude.trim.length == 0){
      return None
    }

    val uuid = getLatLongKey(latitude, longitude)

    if (uuid.length > 0) {
      val cachedObject = lock.synchronized { lru.get(uuid) }

      if(cachedObject != null){

        cachedObject.asInstanceOf[Option[(Location, String, String)]]

      } else {

        val map = Config.persistenceManager.get(uuid, columnFamily)
        map match {
          case Some(map) => {
            val location = new Location
            location.decimalLatitude = latitude
            location.decimalLongitude = longitude

            val el = map.getOrElse("el", "")
            val cl = map.getOrElse("cl", "")

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
    } else {
      None
    }
  }
}
