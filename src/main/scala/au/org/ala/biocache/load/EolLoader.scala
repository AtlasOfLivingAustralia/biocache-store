package au.org.ala.biocache.load

import java.net.{URL, URLEncoder}

import au.org.ala.biocache.model.{FullRecord, Multimedia}
import au.org.ala.biocache.util.OptionParser
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.util.parsing.json.JSON

/**
  * Companion object
  */
object EolLoader {

  def main(args: Array[String]): Unit = {

    var dataResourceUid = ""
    var url = ""

    val parser = new OptionParser("Load EOL images for registered users") {
      arg("<data resource UID>", "The UID of the data resource to load", { v: String => dataResourceUid = v })
      arg("<url>", "The url to scientific names load", { v: String => url = v })
    }

    if (parser.parse(args)) {
      val l = new EolLoader
      l.load(dataResourceUid, url)
    }
  }
}

/**
  * Prototype loader for EOL images.x
  */
class EolLoader extends DataLoader {

  override val logger = LoggerFactory.getLogger("EolLoader")

  def load(dataResourceUid: String, url: String) {
    //val url = "http://130.56.248.115/solr/bie/select?wt=csv&q=rank:Species&fl=scientificName&rows=100"
    val names = Source.fromURL(url).getLines()

    names.foreach(name => {
      logger.info(s"Loading data for $name")
      val nameEncoded = URLEncoder.encode(name, "UTF-8")
      val searchURL = s"http://eol.org/api/search/1.0.json?q=$nameEncoded&page=1&exact=true&filter_by_taxon_concept_id=&filter_by_hierarchy_entry_id=&filter_by_string=&cache_ttl="
      val searchText = Source.fromURL(searchURL).getLines().mkString
      JSON.parseFull(searchText) match {
        case Some(payload) => {
          val results = payload.asInstanceOf[Map[String, List[Map[String, Any]]]].get("results")
          if (!results.isEmpty && results.get.size > 0) {
            val result = results.get.head
            val pageId = result.getOrElse("id", -1).asInstanceOf[Double].toInt
            logger.info(s"For $name pageId: $pageId")
            try {
              loadPage(dataResourceUid, pageId.toString)
            } catch {
              case e: Exception => logger.error(s"Problem loading page id: $pageId " + e.getMessage)
            }
          }
        }
        case None => Nil
      }
    })
  }

  def loadPage(dataResourceUid: String, pageId: String): Unit = {
    //get the ID
    val pageUrl = s"http://eol.org/api/pages/1.0/$pageId.json?images=10&videos=0&sounds=0&maps=0&text=2&iucn=false&subjects=overview&licenses=all&details=true&common_names=true&synonyms=true&references=true&vetted=0&cache_ttl="
    //get the images
    val searchText = Source.fromURL(pageUrl).getLines().mkString
    JSON.parseFull(searchText) match {
      case Some(payload) => {
        val scientificName = payload.asInstanceOf[Map[String, List[Map[String, Any]]]].get("scientificName").get.asInstanceOf[String]
        val dataObjects = payload.asInstanceOf[Map[String, List[Map[String, Any]]]].get("dataObjects")
        if (!dataObjects.isEmpty && dataObjects.get.size > 0) {
          dataObjects.get.foreach { dataObject =>
            val dataType = dataObject.getOrElse("dataType", "").asInstanceOf[String]
            val mediaURL = dataObject.getOrElse("mediaURL", "").asInstanceOf[String]
            val description = dataObject.getOrElse("description", "").asInstanceOf[String]
            val rightsHolder = dataObject.getOrElse("rightsHolder", "").asInstanceOf[String]
            val title = dataObject.getOrElse("title", "").asInstanceOf[String]
            val mimeType = dataObject.getOrElse("mimeType", "").asInstanceOf[String]
            val license = dataObject.getOrElse("license", "").asInstanceOf[String]
            val source = dataObject.getOrElse("source", "").asInstanceOf[String]
            val vettedStatus = dataObject.getOrElse("vettedStatus", "").asInstanceOf[String]
            val identifier = dataObject.getOrElse("identifier", "").asInstanceOf[String]
            val agents = dataObject.getOrElse("agents", List()).asInstanceOf[List[Map[String, String]]]
            var photographer = ""
            agents.foreach { agent =>
              val fullName = agent.getOrElse("full_name", "")
              val role = agent.getOrElse("role", "")
              logger.info(s"agent $fullName")
              if ("photographer" == role) {
                photographer = fullName
              }
            }

            if (dataType == "http://purl.org/dc/dcmitype/StillImage") {
              val fullRecord = new FullRecord()
              fullRecord.classification.scientificName = scientificName
              load(dataResourceUid, fullRecord, List(identifier), List(new Multimedia(new URL(mediaURL), mimeType,
                Map(
                  "license" -> license,
                  "rightsHolder" -> rightsHolder,
                  "title" -> title,
                  "description" -> description,
                  "creator" -> photographer,
                  "source" -> source,
                  "vettedStatus" -> vettedStatus
                )
              )))
              logger.info(s"URL $mediaURL Type: $dataType")
            }
          }
        }
      }
      case None => Nil
    }
  }
}
