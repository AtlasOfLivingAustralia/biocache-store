package au.org.ala.biocache.load

import org.slf4j.LoggerFactory
import java.net.URLEncoder
import au.org.ala.biocache.Config
import au.org.ala.biocache.util.{Json, HttpUtil}
import scala.collection.mutable.ArrayBuffer
import au.org.ala.biocache.cmd.Tool

/**
 * Loader for BVP data.
 */
object BVPLoader extends Tool {

  def cmd = "volunteer-ingest"

  def desc = "Harvests data from the volunteer portal"

  def main(args:Array[String]){
    val b = new BVPLoader
    val drsList = b.retrieveList
    println(drsList)
    drsList.foreach(drUid => IngestTool.ingestResource(drUid))
  }
}

class BVPLoader {

  val logger = LoggerFactory.getLogger("BVPLoader")

  def retrieveList : Seq[String] = {

    val src = scala.io.Source.fromURL(Config.volunteerUrl + "/ws/harvest", "UTF-8").mkString
    val json = scala.util.parsing.json.JSON.parseFull(src)
    val drsToHarvest = new ArrayBuffer[String]()
    try {
      json match {
        case Some(expeditions: Seq[Map[String, Any]]) => {
          expeditions.foreach(exp => {
            //find the expedition, if it doesnt exist create it.
            val resourceUrl = exp.getOrElse("url", "").toString
            var updateUrl = ""

            lookupDataResource(resourceUrl) match {
              case Some(drUid) => {
                println("Updating resource  " + drUid)
                //update
                updateUrl = Config.registryUrl + "/dataResource/" + drUid
              }
              case None => {
                println("Creating resource")
                //create resource
                updateUrl = Config.registryUrl + "/dataResource/"
              }
            }

            //http post to
            val dataUrl = Config.volunteerUrl + "/ajax/expeditionBiocacheData/" + exp.get("id").get.asInstanceOf[Double].toInt
            val dataResourceMap = Map(
              "guid" -> resourceUrl,
              "name" -> exp.getOrElse("name", "").toString,
              "pubDescription" -> exp.getOrElse("description", "").toString,
              "provenance" -> "Draft",
              "websiteUrl" -> exp.getOrElse("url", "").toString,
              "techDescription" -> "This data resource is harvested periodically into the main Atlas index.",
              "resourceType" -> "records",
              "status" -> "dataAvailable",
              "contentTypes" -> Array("point occurrence data"),
              "connectionParameters" -> s"""{"protocol":"DwC", "csv_eol":"\\n", "csv_delimiter": ",", "automation":"true","termsForUniqueKey":["catalogNumber"],"url":"$dataUrl"}"""
            )
            val (respCode, respBody) = HttpUtil.postBody(updateUrl, "application/json", Json.toJSON(dataResourceMap))

            lookupDataResource(resourceUrl) match {
              case Some(uid) => drsToHarvest += uid
              case None => logger.error("Unable to harvest " + resourceUrl + ". Problem registering.")
            }
          })
        }
      }
    } catch {
      case e: Exception => logger.error("Unable to retrieve list of expeditions", e)
    }
    drsToHarvest.toSeq
  }

  /**
   * Retrieve the UID for a resource give its GUID (URL).
   * @param resourceUrl
   * @return
   */
  def lookupDataResource(resourceUrl:String) : Option[String] = {
    val queryUrl = Config.registryUrl + "/lookup/findResourceByGuid?guid=" + URLEncoder.encode(resourceUrl, "UTF-8")
    val drSrc = scala.io.Source.fromURL(queryUrl, "UTF-8").mkString
    val drJson = scala.util.parsing.json.JSON.parseFull(drSrc)
    drJson match {
      case Some(drMetadata:List[Any]) => {
        val drProperties = drMetadata.head
        Some(drProperties.asInstanceOf[List[Map[String,String]]].head.getOrElse("uid",""))
      }
      case None => None
    }
  }
}
