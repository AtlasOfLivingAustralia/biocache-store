package au.org.ala.biocache.load

import java.util.Date

import au.org.ala.biocache
import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.{CMD2, NoArgsTool, Tool}
import au.org.ala.biocache.parser.DateParser
import au.org.ala.biocache.util.{OptionParser, StringHelper}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions
import scala.collection.mutable.HashMap
import scala.io.Source
import scala.util.parsing.json.JSON

/**
  * Runnable loader that just takes a resource UID and delegates based on the protocol.
  */
object Loader extends Tool {

  val logger = LoggerFactory.getLogger("Loader")

  def cmd = "load"

  def desc = "Load a data resource"

  def main(args: Array[String]) {

    var dataResourceUid: String = ""
    var forceLoad = false
    var testLoad = false
    var loadAll = false
    var removeNullFields = false
    var loadMissingOnly = false
    var lastUpdated: Option[Date] = None
    var localFilePath: Option[String] = None
    var logRowKeys = false
    var testFile = false
    var bypassConnParamLookup = false

    val parser = new OptionParser(help) {
      arg("data-resource-uid", "The data resource to load. Supports a comma separated list. Specify 'all' to load all", {
        v: String =>
          if (v == "all") {
            loadAll = true
          } else {
            dataResourceUid = v
          }
      })
      opt("fl", "force-load", "Force the (re)load of media", {
        forceLoad = true
      })
      opt("t", "test-load", "Test the (re)load of media", {
        testLoad = true
      })
      opt("rnf", "remove-null-fields", "Remove the null/Empty fields currently exist in the atlas", {
        removeNullFields = true
      })
      opt("lu", "lastUpdated", "(FlickerLoader only) A limit to load last updated in yyyy-MM-dd format", {
        v: String => lastUpdated = DateParser.parseStringToDate(v)
      })
      opt("l", "local", "(DwCALoader only) skip the download and use local file", { v: String => localFilePath = Some(v) })
      booleanOpt("b", "bypassConnParamLookup", "(DwCALoader only) bypass connection parameter lookup in the registry (collectory)", {
        v: Boolean => bypassConnParamLookup = v
      }
      )
      opt("log", "(DwCALoader only) log row keys to file - allows processing/indexing of changed records", {
        logRowKeys = true
      })
      opt("lmo", "load-missing-only", "Load missing records only", { loadMissingOnly = true })

    }

    Config.indexDAO.init


    if (parser.parse(args)) {
      if (loadAll) {
        val l = new Loader
        l.resourceList.foreach { resource => {
          val uid = resource.getOrElse("uid", "")
          val name = resource.getOrElse("name", "")
          logger.info(s"Loading resource $name, uid: $uid")
          if (uid != "") {
            try {
              l.load(uid, testLoad, forceLoad, removeNullFields, None, localFilePath, bypassConnParamLookup, logRowKeys, loadMissingOnly)
            } catch {
              case e: Exception => logger.error(s"Unable to load data resource with $uid. Exception message: $e.getMessage", e)
            }
          }
        }
        }
      } else {

        logger.info("Starting to load resource: " + dataResourceUid)
        val listOfResources = dataResourceUid.split(",").map(uid => uid.trim())
        val l = new Loader
        listOfResources.foreach {
          l.load(_, testLoad, forceLoad, removeNullFields, None, localFilePath, bypassConnParamLookup, logRowKeys, loadMissingOnly)
        }
        logger.info("Completed loading resource: " + dataResourceUid)
      }
    }
  }
}

object Healthcheck extends NoArgsTool {
  def cmd = "healthcheck"

  def desc = "Run a health check on the configured resources"

  def main(args: Array[String]) = proceed(args, () => (new Loader()).healthcheck)
}

object ListResources extends NoArgsTool {
  def cmd = "list"

  def desc = "List configured data resources"

  def main(args: Array[String]) = proceed(args, () => (new Loader()).printResourceList)
}

object DescribeResource extends Tool {

  def cmd = "describe"

  def desc = "Describe the configuration for a data resource"

  def main(args: Array[String]) {
    var dataResourceUid: String = ""
    val parser = new OptionParser(help) {
      arg("data-resource-uid", "The UID data resource to process, e.g. dr1", { v: String => dataResourceUid = v })
    }

    if (parser.parse(args)) {
      println("Starting to load resource: " + dataResourceUid)
      val l = new Loader
      l.describeResource(List(dataResourceUid))
      println("Completed loading resource: " + dataResourceUid)
      biocache.Config.persistenceManager.shutdown
    }
  }
}

class Loader extends DataLoader {

  import StringHelper._

  import JavaConversions._

  def describeResource(drlist: List[String]) {
    drlist.foreach(dr => {
      retrieveConnectionParameters(dr) match {
        case None => println("Unable to retrieve details of " + dr)
        case Some(dataResourceConfig) =>
          println("UID: " + dr)
          println("This data resource was last checked " + dataResourceConfig.dateLastChecked)
          println("Protocol: " + dataResourceConfig.protocol)
          println("URL: " + dataResourceConfig.urls.mkString(";"))
          println("Unique terms: " + dataResourceConfig.uniqueTerms.mkString(","))
          dataResourceConfig.connectionParams.foreach {
            println(_)
          }
          dataResourceConfig.customParams.foreach {
            println(_)
          }
          println("---------------------------------------")
      }
    })
  }

  def printResourceList {
    if (!resourceList.isEmpty) {
      CMD2.printTable(resourceList)
    } else {
      println("No resources are registered in the registry.")
    }
  }

  def resourceList: List[Map[String, String]] = {
    val json = Source.fromURL(Config.registryUrl + "/dataResource?resourceType=records").getLines.mkString
    val drs = JSON.parseFull(json).get.asInstanceOf[List[Map[String, String]]]
    drs
  }

  def load(dataResourceUid: String, test: Boolean = false, forceLoad: Boolean = false, removeNullFields: Boolean = false,
           startDate: Option[Date] = None, localFilePath: Option[String] = None,
           bypassConnParamLookup: Boolean = false, logRowKeys: Boolean = false, loadMissingOnly: Boolean = false) {
    try {
      val config = retrieveConnectionParameters(dataResourceUid)
      if (config.isEmpty) {
        logger.info("Unable to retrieve connection details for  " + dataResourceUid)
        return
      }

      config.get.protocol.toLowerCase match {
        case "biocase" => {
          logger.info("Loading Biocase provider")
          val l = new BiocaseLoader
          l.load(dataResourceUid, test)
        }
        case "dwc" => {
          logger.info("Darwin core headed CSV loading")
          val l = new DwcCSVLoader
          l.load(dataResourceUid, false, test, forceLoad, removeNullFields)
        }
        case "dwca" => {
          logger.info("Darwin core archive loading")
          val l = new DwCALoader
          l.deleteOldRowKeys(dataResourceUid)
          if (localFilePath.isEmpty) {
            l.load(dataResourceUid, logRowKeys, test, forceLoad, removeNullFields, loadMissingOnly)
          } else {
            if (bypassConnParamLookup) {
              l.loadArchive(localFilePath.get, dataResourceUid, List(), None, false, logRowKeys, test, removeNullFields, loadMissingOnly)
            } else {
              l.loadLocal(dataResourceUid, localFilePath.get, logRowKeys, test, removeNullFields, loadMissingOnly)
            }
          }
          //initialise the delete & update the collectory information
          l.updateLastChecked(dataResourceUid)
        }
        case "digir" => {
          logger.info("digir webservice loading")
          val l = new DiGIRLoader
          if (!test)
            l.load(dataResourceUid)
          else
            println("TESTING is not supported for DiGIR")
        }
        case "flickr" => {
          logger.info("flickr webservice loading")
          val l = new FlickrLoader
          if (!test)
            l.load(dataResourceUid, startDate, None, true, lastUpdatedDate = None)
          else
            println("TESTING is not supported for Flickr")
        }
        case "eol" => {
          logger.info("EOL webservice loading")
          val l = new EolLoader
          if (!test) {
            val speciesListUrl = config.get.connectionParams.getOrElse("species_list_url", "")
            if (speciesListUrl != "") {
              l.load(dataResourceUid, speciesListUrl)
            } else {
              println("speciesListUrl not configured for EOL loader")
            }
          } else {
            println("TESTING is not supported for EOL")
          }
        }
        case "customwebservice" => {
          logger.info("custom webservice loading")
          if (!test) {
            val className = config.get.customParams.getOrElse("classname", null)
            if (className == null) {
              println("Classname of custom harvester class not present in parameters")
            } else {
              val wsClass = Class.forName(className)
              val l = wsClass.newInstance()
              if (l.isInstanceOf[CustomWebserviceLoader]) {
                l.asInstanceOf[CustomWebserviceLoader].load(dataResourceUid)
              } else {
                println("Class " + className + " is not a subtype of au.org.ala.util.CustomWebserviceLoader")
              }
            }
          } else {
            println("TESTING is not supported for custom web service")
          }
        }
        case "autofeed" => {
          logger.info("AutoFeed Darwin core headed CSV loading")
          val l = new AutoDwcCSVLoader
          if (!test)
            l.load(dataResourceUid, forceLoad = forceLoad)
          else
            logger.warn("TESTING is not supported for auto-feed")
        }
        case "nbn" => {
          logger.info("NBN exchange format loading")
          val l = new NBNFormatLoader
          if (!test)
            l.load(dataResourceUid, true)
          else
            println("TESTING is not supported for NBN exchange format")
        }
        case _ => logger.warn("Protocol " + config.get.protocol + " currently unsupported.")
      }
    } catch {
      //NC 2013-05-10: Need to rethrow the exception to allow the tools to allow the tools to pick up on them.
      case e: Exception => logger.error(e.getMessage(), e); throw e
    }

    if (test) {
      println("Check the output for any warning/error messages.")
      println("If there are any new institution and collection codes ensure that they are handled correctly in the Collectory.")
      println("""Don't forget to check that number of NEW records.  If this is high for updated data set it may indicate that the "unique values" have changed format.""")
    }
  }

  def healthcheck = {
    val json = Source.fromURL(Config.registryUrl + "/dataResource/harvesting", "UTF-8").getLines.mkString
    val drs = JSON.parseFull(json).get.asInstanceOf[List[Map[String, String]]]
    // UID, name, protocol, URL,
    val digirCache = new HashMap[String, Map[String, String]]()
    //iterate through the resources
    drs.foreach(dr => {

      val drUid = dr.getOrElse("uid", "")
      val drName = dr.getOrElse("name", "")
      val connParams = dr.getOrElse("connectionParameters", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]

      if (connParams != null) {
        val protocol = connParams.getOrElse("protocol", "").asInstanceOf[String].toLowerCase

        val urlsObject = connParams.getOrElse("url", List[String]())
        val urls: Seq[String] = if (urlsObject.isInstanceOf[Seq[_]]) {
          urlsObject.asInstanceOf[Seq[String]]
        } else {
          List(connParams("url").asInstanceOf[String])
        }

        urls.foreach(url => {
          val status = protocol match {
            case "dwc" => checkArchive(drUid, url)
            case "dwca" => checkArchive(drUid, url)
            case "digir" => {
              if (url == null || url == "") {
                Map("Status" -> "NOT CONFIGURED")
              } else if (!digirCache.get(url).isEmpty) {
                digirCache.get(url).get
              } else {
                val result = checkDigir(drUid, url)
                digirCache.put(url, result)
                result
              }
            }
            case _ => Map("Status" -> "IGNORED")
          }

          if (status.getOrElse("Status", "NO STATUS") != "IGNORED") {

            val fileSize = status.getOrElse("Content-Length", "N/A")
            val displaySize = {
              if (fileSize != "N/A") {
                (fileSize.toInt / 1024).toString + "kB"
              } else {
                fileSize
              }
            }
            println(drUid.fixWidth(5) + "\t" + protocol.fixWidth(8) + "\t" +
              status.getOrElse("Status", "NO STATUS").fixWidth(15) + "\t" + drName.fixWidth(65) + "\t" + url.fixWidth(85) + "\t" +
              displaySize)
          }
        })
      }
    })
  }

  def checkArchive(drUid: String, url: String): Map[String, String] = {
    if (url != "") {
      val conn = (new java.net.URL(url)).openConnection()
      val headers = conn.getHeaderFields()
      val map = new HashMap[String, String]
      headers.foreach({ case (header, values) => {
        map.put(header, values.mkString(","))
      }
      })
      map.put("Status", "OK")
      map.toMap
    } else {
      Map("Status" -> "UNAVAILABLE")
    }
  }

  def checkDigir(drUid: String, url: String): Map[String, String] = {

    try {
      val conn = (new java.net.URL(url)).openConnection()
      val headers = conn.getHeaderFields()
      val hdrs = new HashMap[String, String]()
      headers.foreach({ case (header, values) => {
        hdrs.put(header, values.mkString(","))
      }
      })
      (hdrs + ("Status" -> "OK")).toMap
    } catch {
      case e: Exception => {
        e.printStackTrace
        Map("Status" -> "UNAVAILABLE")
      }
    }
  }
}