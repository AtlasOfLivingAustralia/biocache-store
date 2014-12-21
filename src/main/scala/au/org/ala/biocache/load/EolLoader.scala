package au.org.ala.biocache.load

import java.net.{URLEncoder}

import au.org.ala.biocache.Config
import au.org.ala.biocache.model.FullRecord
import au.org.ala.biocache.util.OptionParser

import scala.io.Source
import scala.util.parsing.json.JSON

/**
 * Companion object
 */
object EolLoader {

  def main(args:Array[String]): Unit = {

    var dataResourceUid = ""
    var url = ""

    val parser = new OptionParser("Load EOL images for registered users") {
      arg("<data resource UID>", "The UID of the data resource to load", { v: String => dataResourceUid = v})
      arg("<url>", "The url to scientific namesload", { v: String => url = v})
    }

    if(parser.parse(args)){
      val l = new EolLoader
      l.load(dataResourceUid, url)
    }
  }
}

/**
 * Prototype loader for EOL images.x
 */
class EolLoader extends DataLoader {
  def load(dataResouceUid:String, url:String) {
    //val url = "http://130.56.248.115/solr/bie/select?wt=csv&q=rank:Species&fl=scientificName&rows=100"
    val names = Source.fromURL(url).getLines()

    names.foreach( name => {
      println(name)
      val nameEncoded = URLEncoder.encode(name, "UTF-8")
      val searchURL = s"http://eol.org/api/search/1.0.json?q=$nameEncoded&page=1&exact=true&filter_by_taxon_concept_id=&filter_by_hierarchy_entry_id=&filter_by_string=&cache_ttl="
      val searchText = Source.fromURL(searchURL).getLines().mkString
      JSON.parseFull(searchText) match {
        case Some(payload) => {
          val results = payload.asInstanceOf[Map[String, List[Map[String, Any]]]].get("results")
          if(!results.isEmpty && results.get.size > 0){
            val result = results.get.head
            val pageId = result.getOrElse("id", -1).asInstanceOf[Double].toInt
            println(s"For $name pageId: $pageId")
            loadPage(dataResouceUid, pageId.toString)
          }
        }
        case None => Nil
      }
    })
  }

  def loadPage(dataResourceUID:String, pageId:String): Unit ={
    //get the ID
    val pageUrl = s"http://eol.org/api/pages/1.0/$pageId.json?images=10&videos=0&sounds=0&maps=0&text=2&iucn=false&subjects=overview&licenses=all&details=true&common_names=true&synonyms=true&references=true&vetted=0&cache_ttl="
    //get the images
    val searchText = Source.fromURL(pageUrl).getLines().mkString
    JSON.parseFull(searchText) match {
      case Some(payload) => {
        val scientificName = payload.asInstanceOf[Map[String, List[Map[String, Any]]]].get("scientificName").get.asInstanceOf[String]
        val dataObjects = payload.asInstanceOf[Map[String, List[Map[String, Any]]]].get("dataObjects")
        if(!dataObjects.isEmpty && dataObjects.get.size > 0){
          dataObjects.get.foreach { dataObject =>
            val dataType = dataObject.getOrElse("dataType", "unknown").asInstanceOf[String]
            val mediaURL = dataObject.getOrElse("mediaURL", "no media URL").asInstanceOf[String]
            val description = dataObject.getOrElse("description", "no description").asInstanceOf[String]
            val rightsHolder = dataObject.getOrElse("rightsHolder", "no rights holder").asInstanceOf[String]
            val title = dataObject.getOrElse("title", "no title").asInstanceOf[String]
            val mimeType = dataObject.getOrElse("mimeType", "no mimeType").asInstanceOf[String]
            val license = dataObject.getOrElse("license", "no license").asInstanceOf[String]
            val identifier = dataObject.getOrElse("identifier", "no license").asInstanceOf[String]
            val agents = dataObject.getOrElse("agents", List()).asInstanceOf[List[Map[String,String]]]
            var photographer = ""
            agents.foreach { agent =>
              val fullName = agent.getOrElse("full_name", "no name for agent")
              val role = agent.getOrElse("role", "no name for agent")
              println(s"agent $fullName" )
              if("photographer" == role){
                photographer = fullName
              }
            }

            if(dataType == "http://purl.org/dc/dcmitype/StillImage") {
              val fullRecord = new FullRecord()
              fullRecord.classification.scientificName = scientificName
              fullRecord.occurrence.photographer = photographer
              fullRecord.occurrence.associatedMedia = mediaURL
              load(dataResourceUID, fullRecord, List(identifier), false, true)
              Config.occurrenceDAO.addRawOccurrence(fullRecord)
              println(s"URL $mediaURL Type: $dataType")
            }
          }
        }
      }
      case None => Nil
    }
  }
}
