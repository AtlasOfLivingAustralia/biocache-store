package au.org.ala.biocache.caches

import java.io.File

import au.org.ala.biocache.Config
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.model.{FullRecord, Versions}
import au.org.ala.sds.SensitiveDataService
import au.org.ala.sds.validation.ValidationOutcome
import org.ala.layers.intersect.SimpleShapeFile
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.parsing.json.JSON

/**
 * A DAO for access to sensitive areas.
 */
object SensitiveAreaDAO {

  import scala.collection.JavaConversions._
  import au.org.ala.biocache.util.StringHelper._

  val logger = LoggerFactory.getLogger("SensitiveAreaDAO")
  private val lru = new org.apache.commons.collections.map.LRUMap(10000)
  private val lock : AnyRef = new Object()

  lazy val sdsFinder = Config.sdsFinder
  val sds = new SensitiveDataService()

  val idNameLookup = new mutable.HashMap[String, String]()
  val nameFieldLookup = new mutable.HashMap[String, String]()
  val loadedShapeFiles = new mutable.HashMap[String, SimpleShapeFile]()

  init

  /**
   * Load polygon layers from the filesystem into memory to support fast intersect
   * queries.
   */
  private def init = {

    //SDS layer loading
    logger.info("Loading Layer information from ....." + Config.layersServiceUrl)
    val layersJson = WebServiceLoader.getWSStringContent(Config.layersServiceUrl + "/layers")
    val fieldsJson = WebServiceLoader.getWSStringContent(Config.layersServiceUrl + "/fields")

    //layers
    val layers = JSON.parseFull(layersJson).getOrElse(List[Map[String,String]]()).asInstanceOf[List[Map[String,String]]]
    logger.info("Number of layers loaded ....." + layers.size)
    layers.foreach(layer => {
      idNameLookup.put(layer.getOrElse("id", -1).asInstanceOf[Double].toInt.toString(), layer.getOrElse("name", ""))
    })

    //fields
    val fields = JSON.parseFull(fieldsJson).getOrElse(List[Map[String,String]]()).asInstanceOf[List[Map[String,String]]]
    logger.info("Number of fields loaded ....." + fields.size)
    fields.foreach(field => {
      nameFieldLookup.put(field.getOrElse("spid", "-1"), field.getOrElse("sname", ""))
    })

    logger.info("Loaded layers required by SDS.....")
    //load SDS layer metadata
    au.org.ala.sds.util.GeoLocationHelper.getGeospatialLayers.foreach(layerID => {
      //check each layer is available on the local filesystem
      loadLayer(layerID, true)
    })

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
  def intersect(decimalLongitude:String, decimalLatitude:String) : Map[String, String] = {
    val optLong = decimalLongitude.toDoubleWithOption
    val optLat = decimalLatitude.toDoubleWithOption
    if(!optLong.isEmpty && !optLong.isEmpty){
      intersect(optLong.get, optLat.get)
    } else {
      Map[String,String]()
    }
  }
  
  /**
   * Do a point intersect lookup for SDS layers.
   *
   * @param decimalLongitude
   * @param decimalLatitude
   * @return
   */
  def intersect(decimalLongitude:Double, decimalLatitude:Double) : Map[String, String] = {

    if(decimalLongitude == null || decimalLatitude == null){
      return Map[String,String]()
    }

    val guid = getLatLongKey(decimalLongitude, decimalLatitude)

    //retrieve from cache
    val lookup = lock.synchronized { lru.get(guid) }

    if(lookup != null){
      lookup.asInstanceOf[Map[String,String]]
    } else {
      val intersects = new mutable.HashMap[String ,String]()
      loadedShapeFiles.foreach { case (layerID, shp) =>
        val intersectValue = shp.intersect(decimalLongitude, decimalLatitude)
        if(intersectValue != null) {
          intersects.put(layerID, intersectValue)
        }
      }
      val intersectMap = intersects.toMap
      lock.synchronized { lru.put(guid, intersectMap) }
      intersectMap
    }
  }

  def add(decimalLongitude:Double, decimalLatitude:Double, intersectValues:Map[String,String]): Unit = {
    lock.synchronized { lru.put(getLatLongKey(decimalLongitude, decimalLatitude), intersectValues) }
  }

  private def getLatLongKey(longitude:Double, latitude:Double) : String = {
    longitude.toString +  "|" + latitude.toString
  }
}
