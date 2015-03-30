package au.org.ala.biocache.caches

import java.io.File

import au.org.ala.biocache.Config
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.model.{FullRecord, Versions}
import au.org.ala.sds.SensitiveDataService
import au.org.ala.sds.validation.ValidationOutcome
import org.ala.layers.intersect.SimpleShapeFile
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.parsing.json.JSON

/**
 * A DAO for access to sensitive areas.
 */
object SensitiveAreaDAO {

  import scala.collection.JavaConversions._

  val logger = LoggerFactory.getLogger("SensitiveAreaDAO")
  private val lru = new org.apache.commons.collections.map.LRUMap(10000)
  private val lock : AnyRef = new Object()

  lazy val sdsFinder = Config.sdsFinder
  val sds = new SensitiveDataService()

  val idNameLookup = new mutable.HashMap[String, String]()
  val nameFieldLookup = new mutable.HashMap[String, String]()
  val loadedShapeFiles = new mutable.HashMap[String, SimpleShapeFile]()

  init

  private def init = {
    logger.info("Loading Layer information from ....." + Config.layersServiceUrl)
    val layersJson = WebServiceLoader.getWSStringContent(Config.layersServiceUrl + "/layers")
    val fieldsJson = WebServiceLoader.getWSStringContent(Config.layersServiceUrl + "/fields")

    //layers
    val layers = JSON.parseFull(layersJson).getOrElse(List[Map[String,String]]()).asInstanceOf[List[Map[String,String]]]
    layers.foreach(layer => {
      idNameLookup.put(layer.getOrElse("uid", "-1"), layer.getOrElse("name", ""))
    })

    //fields
    val fields = JSON.parseFull(fieldsJson).getOrElse(List[Map[String,String]]()).asInstanceOf[List[Map[String,String]]]
    fields.foreach(field => {
      nameFieldLookup.put(field.getOrElse("spid", "-1"), field.getOrElse("sname", ""))
    })

    //load layer metadata
    au.org.ala.sds.util.GeoLocationHelper.getGeospatialLayers.foreach(layerID => {
      //check each layer is available on the local filesystem
      val id = layerID.replaceAll("cl","")
      val f = new File(Config.layersDirectory + idNameLookup.getOrElse(id, "xxxxx") + ".shp")
      logger.info("Geospatial ID..." + layerID + " - available: " + f.exists() + " - " + f.getAbsolutePath + ", field name: " + nameFieldLookup.getOrElse(id, "xxxxx"))
      if(f.exists()){
        loadedShapeFiles.put(layerID, new SimpleShapeFile(Config.layersDirectory  + idNameLookup.getOrElse(id, "xxxxx"), nameFieldLookup.getOrElse(id, "xxxxx")))
      }
    })
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
    val lookup = lru.get(guid)

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

  private def getLatLongKey(latitude:Double, longitude:Double) : String = {
    latitude.toString.trim + "|" + longitude.toString
  }
}
