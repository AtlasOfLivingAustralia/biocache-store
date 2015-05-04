package au.org.ala.biocache.util

import java.io.{FileOutputStream, File}
import java.net.URL
import javax.xml.transform.TransformerFactory
import javax.xml.transform.stream.{StreamResult, StreamSource}

import scala.xml.{Source, XML}


/**
 * Collect the IANA media type information from http://www.iana.org/assignments/media-types/media-types.xml
 * and convert it into a media type map.
 *
 * @author Doug Palmer &lt;Doug.Palmer@csiro.au&gt;
 *
 *         Copyright (c) 2015 CSIRO
 */
object MediaTypeRetreival {
  val DEFAULT_URL = new URL("http://www.iana.org/assignments/media-types/media-types.xml")
  val DEFAULT_OUTPUT = new File("src/main/resources/mime-types.txt")
  val STYLESHEET = "media-type.xsl"


  def main(args: Array[String]): Unit = {
    val source = new StreamSource(DEFAULT_URL.openStream())
    val stylesheet = new StreamSource(this.getClass.getResourceAsStream(STYLESHEET))
    val transformer = TransformerFactory.newInstance().newTransformer(stylesheet)
    val result = new StreamResult(new FileOutputStream(DEFAULT_OUTPUT))
    transformer.transform(source, result)
    println("Your pain is not over yet. You will need to manually edit the resulting file to get rid of synonyms that Vocab doesn't like")
    println("In particualar video/JPEG and image/jpeg")
    println("We await your return with interest")
  }
}
