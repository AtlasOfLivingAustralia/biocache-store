package au.org.ala.biocache.caches

import java.io.File

import au.org.ala.biocache.Config
import au.org.ala.biocache.model.FullRecord
import au.org.ala.biocache.util.StringHelper
import au.org.ala.layers.intersect.SimpleShapeFile
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.parsing.json.JSON

/**
 * A DAO for access to spatial areas.
 */
object SpatialLayerDAO {

  import StringHelper._

  val logger = LoggerFactory.getLogger("SpatialLayerDAO")

  val idNameLookup = new mutable.HashMap[String, String]()
  val nameFieldLookup = new mutable.HashMap[String, String]()
  val loadedShapeFiles = new mutable.HashMap[String, SimpleShapeFile]()
  private val lock : AnyRef = new Object()
  private val lru = new org.apache.commons.collections.map.LRUMap(Config.spatialCacheSize)
  var sdsLayerList:List[String] = List()

  /**
   * Load polygon layers from the filesystem into memory to support fast intersect
   * queries.
   */
  lazy val init = {

    if (StringUtils.isNotEmpty(Config.layersServiceUrl) && (Config.layersServiceSampling || Config.sdsEnabled)){

      //SDS layer loading
      logger.info("Loading Layer information from ....." + Config.layersServiceUrl)
      val layersJson = WebServiceLoader.getWSStringContent(Config.layersServiceUrl + "/layers")
      val fieldsJson = WebServiceLoader.getWSStringContent(Config.layersServiceUrl + "/fields")

      //layers
      val layers = JSON.parseFull(layersJson).getOrElse(List[Map[String, String]]()).asInstanceOf[List[Map[String, String]]]
      logger.info("Number of layers loaded ....." + layers.size)
      layers.foreach { layer =>
        idNameLookup.put(layer.getOrElse("id", -1).asInstanceOf[Double].toInt.toString(), layer.getOrElse("name", ""))
      }

      //fields
      val fields = JSON.parseFull(fieldsJson).getOrElse(List[Map[String, String]]()).asInstanceOf[List[Map[String, String]]]
      logger.info("Number of fields loaded ....." + fields.size)
      fields.foreach { field =>
        nameFieldLookup.put(field.getOrElse("spid", "-1"), field.getOrElse("sname", ""))
      }
    }

    logger.info("SDS enabled ....." + Config.sdsEnabled)

    //load SDS layer metadata
    if (Config.sdsEnabled) {

      logger.info("Loaded layers required by SDS.....")
      val sdsLayerListURL = Config.sdsUrl + "/ws/layers"
      logger.info("Retrieving list from " + sdsLayerListURL)

      val sdsListJson = WebServiceLoader.getWSStringContent(sdsLayerListURL)
      sdsLayerList = JSON.parseFull(sdsListJson).getOrElse(List[String]()).asInstanceOf[List[String]]

      sdsLayerList.foreach { layerID =>
        //check each layer is available on the local filesystem
        logger.info("Loading SDS layer....." + layerID)
        loadLayer(layerID, true)
      }
    } else {
      logger.info("SDS disabled - skipping layer loading.....")
    }

    //Load additional layers
    if(StringUtils.isNotBlank(Config.stateProvinceLayerID)){
      loadLayer(Config.stateProvinceLayerID, false)
    }
    if(StringUtils.isNotBlank(Config.terrestrialLayerID)){
      loadLayer(Config.terrestrialLayerID, false)
    }
    if(StringUtils.isNotBlank(Config.marineLayerID)){
      loadLayer(Config.marineLayerID, false)
    }
    if(StringUtils.isNotBlank(Config.countriesLayerID)){
      loadLayer(Config.countriesLayerID, false)
    }
    if(StringUtils.isNotBlank(Config.localGovLayerID)){
      loadLayer(Config.localGovLayerID, false)
    }

    true
  }

  private def loadLayer(layerID: String, errorIfNotAvailable:Boolean = false): Unit ={
    val id = layerID.replaceAll("cl","")
    val f = new File(Config.layersDirectory + idNameLookup.getOrElse(id, "xxxxx") + ".shp")
    logger.info("Geospatial ID..." + layerID +
      " - available: " + f.exists() +
      " - " + f.getAbsolutePath +
      ", field name: " + nameFieldLookup.getOrElse(id, "xxxxx")
    )
    if(f.exists()){
      val ssf = new SimpleShapeFile(Config.layersDirectory + idNameLookup.getOrElse(id, "xxxxx"), nameFieldLookup.getOrElse(id, "xxxxx"))
      loadedShapeFiles.put(layerID, ssf)
    } else if(errorIfNotAvailable) {
      throw new RuntimeException(s"Layer $layerID unavailable on local filesystem. " +
        "To disable SDS checking set sds.enabled=false in your external config file." )
    }
  }

  /**
   * Intersect the supplied coordinates with loaded layers.
   *
   * @param decimalLongitude
   * @param decimalLatitude
   * @return
   */
  def intersect(decimalLongitude:String, decimalLatitude:String) : collection.Map[String, String] = {
    val optLong = decimalLongitude.toDoubleWithOption
    val optLat = decimalLatitude.toDoubleWithOption
    if(!optLong.isEmpty && !optLong.isEmpty){
      intersect(optLong.get, optLat.get)
    } else {
      Map[String,String]()
    }
  }

  /**
    * Do a point intersect lookup for layers.
    *
    * @param fr
    * @return
    */
  def intersect(fr:FullRecord) : collection.Map[String, String] = {

    if(fr.location.decimalLongitude == null || fr.location.decimalLatitude == null){
      return Map[String,String]()
    }

    intersect(fr.location.decimalLongitude.toDouble,
      fr.location.decimalLatitude.toDouble)
  }

  /**
   * Do a point intersect lookup for the configured layers.
   *
   * @param decimalLongitude
   * @param decimalLatitude
   * @return
   */
  def intersect(decimalLongitude:Double, decimalLatitude:Double) : collection.Map[String, String] = {
    init

    if(decimalLongitude == null || decimalLatitude == null){
      return Map[String,String]()
    }

    val key = decimalLongitude + "|" + decimalLatitude
    val cachedObject = lock.synchronized { lru.get(key) }

    if(cachedObject != null){
      cachedObject.asInstanceOf[collection.Map[String, String]]
    } else {
      val intersects = new mutable.HashMap[String ,String]()
      loadedShapeFiles.foreach { case (layerID, shp) =>
        val intersectValue = shp.intersect(decimalLongitude, decimalLatitude)
        if(intersectValue != null) {
          intersects.put(layerID, intersectValue)
        }
      }
      lock.synchronized { lru.put(key, intersects) }
      intersects
    }
  }

  private def getLatLongKey(longitude:Double, latitude:Double) : String = {
    longitude.toString +  "|" + latitude.toString
  }

  def getCacheSize = lru.size()
}