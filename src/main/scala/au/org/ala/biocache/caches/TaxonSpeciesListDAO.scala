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

  private var speciesListColumnsMap:Map[String, Map[String, String]] = _

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
    * Get the list details for the supplied guid
    * @param conceptLsid
    * @return
    */
  def getCachedColumnsForTaxon(conceptLsid:String) : Map[String, String] = {

    if( StringUtils.isEmpty(Config.listToolUrl) ){
      return (Map.empty)
    }

    //check to see if the species list map has been initialised
    if(speciesListColumnsMap == null){
      lock.synchronized {
        //only le the first thread actually perform the init by rechecking the null
        if(speciesListColumnsMap == null){
          if(StringUtils.isNotEmpty(Config.listToolUrl)){
            logger.info("Loading list information...")
            speciesListColumnsMap = buildTaxonListColumnsMap
            logger.info("Loading list information...completed.")
          } else {
            logger.info("List tool URL is not configured. Skipping list loading...")
          }
        }
      }
    }

    //now return the the values
    speciesListColumnsMap.getOrElse(conceptLsid, Map[String, String]())
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
          if (guid.length() > 0) {
            val specieslists = guidMap.getOrElse(guid, List[String]())
            if (specieslists.isEmpty) {
              guidMap.put(guid, List(drUid))
            } else if (!specieslists.contains(drUid)) {
              guidMap.put(guid, specieslists :+ drUid)
            }
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
    * Build a map of taxonID -> (list UID + header -> value).
    *
    * @return
    */
  def buildTaxonListColumnsMap : Map[String, Map[String, String]] = {

    try {
      //build up a map of taxonID -> list of species lists...
      val guidMap = new mutable.HashMap[String, Map[String, String]]()

      getListUids.foreach { drUid =>

        try {
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
            if (guid.length() > 0) {
              val item = new mutable.HashMap[String, String]()
              for (i <- 0 until columnHdrs.length) {
                item.put(drUid + "_" + columnHdrs(i), currentLine(i))
              }

              val items = guidMap.getOrElse(guid, mutable.HashMap[String, String]())
              if (items.isEmpty) {
                guidMap.put(guid, item.toMap[String, String])
              } else {
                guidMap.put(guid, (items ++ item).toMap[String, String])
              }
            }

            currentLine = csvReader.readNext()
          }
        } catch {
          case e:Exception => {
            logger.error("failed to get a list $drUid", e)
          }
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

    val listsUrl = Config.listToolUrl + "/ws/speciesList?isAuthoritative=eq:true&max=1000"
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
}
