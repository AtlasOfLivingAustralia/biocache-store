package au.org.ala.biocache.caches

import org.slf4j.LoggerFactory

import scala.io.Source

object WebServiceLoader {

  val logger = LoggerFactory.getLogger("WebServiceLoader")

  def getWSStringContent(url: String) = try {
    Source.fromURL( url, "UTF-8" ).mkString
  } catch {
    case e: Exception => {
      logger.warn(s"Unable to load $url : " + e.getMessage)
      ""
    }
  }
}