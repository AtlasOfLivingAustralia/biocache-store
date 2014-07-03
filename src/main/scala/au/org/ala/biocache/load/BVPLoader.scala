package au.org.ala.biocache.load

import org.slf4j.LoggerFactory
import java.net.URLEncoder
import au.org.ala.biocache.Config
import au.org.ala.biocache.util.{OptionParser, Json, HttpUtil}
import scala.collection.mutable.ArrayBuffer
import au.org.ala.biocache.cmd.Tool

/**
 * Loader for BVP data.
 */
object BVPLoader extends Tool {

  val logger = LoggerFactory.getLogger("BVPLoader")

  def cmd = "volunteer-ingest"

  def desc = "Harvests data from the volunteer portal"

  def main(args:Array[String]){

    var ingestResources = false
    var syncOnly = false
    var debugOnly = false
    var startAt = ""

    val parser = new OptionParser(help) {
      opt("debug", "Display the list of expeditions. For debug purposes.", {
        debugOnly = true
      })
      opt("sync-only", "synchronise the list of data resources. Dont ingest.", {
        syncOnly = true
      })
      opt("i", "ingest", "flag to indicate all resources should be loaded", {
        ingestResources = true
      })
      opt("sa", "start-at-uid", "Start ingesting resources at the supplied UID", {
        v: String => startAt = v
      })
    }
    if(parser.parse(args)){
      val b = new BVPLoader
      if(debugOnly){
        b.displayList
      } else {
        val drsList = b.retrieveList
        logger.info("Number of resources to harvest: " + drsList.size)
        if (logger.isDebugEnabled) {
          drsList.foreach(dr => logger.debug("Will harvest: " + dr))
        }

        //start at UID
        val drsToIngest:Seq[String] = if(startAt != ""){
          val idx = drsList.indexOf(startAt)
          if(idx>0){
            drsList.drop(idx)
          } else {
            drsList
          }
        } else {
          drsList
        }

        if (ingestResources && !syncOnly) {
          drsToIngest.foreach(drUid =>
            try {
              IngestTool.ingestResource(drUid)
            } catch {
              case e:Exception => logger.error(e.getMessage, e)
            }
          )
        }
      }
    }
  }
}

class BVPLoader {

  val logger = LoggerFactory.getLogger("BVPLoader")

  def displayList {
    val src = scala.io.Source.fromURL(Config.volunteerUrl + "/ws/harvest", "UTF-8").mkString
    val json = scala.util.parsing.json.JSON.parseFull(src)
    try {
      json match {
        case Some(expeditions: Seq[Map[String, Any]]) => {
          expeditions.foreach(exp => {
            logger.info(exp.getOrElse("url", "").toString + " - " + exp.getOrElse("name", "").toString)
          })
        }
        case None => {
          logger.info("No expeditions listed")
        }
      }
    } catch {
      case e: Exception => logger.error("Unable to retrieve list of expeditions", e)
    }
  }

  def retrieveList : Seq[String] = {

    val stripHtmlRegex = "\\<.*?\\>"
    val src = scala.io.Source.fromURL(Config.volunteerUrl + "/ws/harvest", "UTF-8").mkString
    val json = scala.util.parsing.json.JSON.parseFull(src)
    val drsToHarvest = new ArrayBuffer[String]()
    try {
      json match {
        case Some(expeditions: Seq[Map[String, Any]]) => {
          expeditions.foreach(exp => {
            //find the expedition, if it doesnt exist create it.
            val resourceUrl = exp.getOrElse("url", "").toString
            val updateUrl = lookupDataResource(resourceUrl) match {
              case Some(drUid) => {
                logger.info("Updating resource  " + drUid + ", URL: " + resourceUrl)
                //update
                Config.registryUrl + "/dataResource/" + drUid
              }
              case None => {
                logger.info("Creating resource for " + resourceUrl)
                //create resource
                Config.registryUrl + "/dataResource/"
              }
            }

            val tasksCount = exp.getOrElse("tasksCount", "").asInstanceOf[Double].toInt
            val tasksTranscribedCount = exp.getOrElse("tasksTranscribedCount", "").asInstanceOf[Double].toInt
            val tasksValidatedCount = exp.getOrElse("tasksValidatedCount", "").asInstanceOf[Double].toInt
            var description = exp.getOrElse("description", "").toString.replaceAll(stripHtmlRegex,"")
            val validationTag = if(tasksCount == tasksValidatedCount){
              description = description +  s"This expedition of $tasksCount tasks is fully transcribed and validated."
              "validation complete"
            } else {
              description = description +  s" The total number of tasks for this dataset is: $tasksCount, number transcribed is  $tasksTranscribedCount and number validated is $tasksValidatedCount."
              "validation in-progress"
            }

            val transcriptionTag = if(tasksCount == tasksTranscribedCount){
              "transcribing complete"
            } else {
              "transcribing in-progress"
            }

            //http post to
            val dataUrl = Config.volunteerUrl + "/ajax/expeditionBiocacheData/" + exp.get("id").get.asInstanceOf[Double].toInt
            val dataResourceMap = Map(
              "api_key"-> Config.collectoryApiKey,
              "guid" -> resourceUrl,
              "name" -> exp.getOrElse("name", "").toString,
              "pubDescription" -> description,
              "provenance" -> "Draft",
              "websiteUrl" -> exp.getOrElse("url", "").toString,
              "techDescription" -> "This data resource is harvested periodically into the main occurrence index. ",
              "resourceType" -> "records",
              "status" -> "dataAvailable",
              "contentTypes" -> Array("point occurrence data", "crowd sourced", "BVP", transcriptionTag, validationTag),
              "connectionParameters" -> s"""{"protocol":"DwC", "csv_eol":"\\n", "csv_delimiter": ",", "automation":"true","termsForUniqueKey":["catalogNumber"],"url":"$dataUrl"}"""
            )
            val (respCode, respBody) = HttpUtil.postBody(updateUrl, "application/json", Json.toJSON(dataResourceMap))

            logger.info("Response code for " + resourceUrl + ". Code: "  + respCode)

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
    val drs = drsToHarvest.toArray

    if(Config.volunteerHubUid != "") {
      val (respCode, respBody) = HttpUtil.postBody(Config.registryUrl + "/dataHub/" + Config.volunteerHubUid, "application/json", Json.toJSON(Map(
        "api_key" -> Config.collectoryApiKey,
        "memberDataResources" -> Json.toJSON(drs)
      )))
      logger.info("Data hub sync: " + respCode)
      logger.info("Data hub sync response : " + respBody)
    } else {
      logger.info("Data hub sync skipped. Please create a hub entry in the registry and configure the biocache to use it.")
    }
      drs
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
        if(!drMetadata.isEmpty){
          val drProperties = drMetadata.head
          Some(drProperties.asInstanceOf[List[Map[String,String]]].head.getOrElse("uid",""))
        } else {
          None
        }
      }
      case None => None
    }
  }
}
