package au.org.ala.biocache.model

import java.net.URL

import au.org.ala.biocache.vocab.MimeType
import org.gbif.dwc.terms.{DcTerm, Term}

import scala.util.matching.Regex
import scala.xml.XML
import scala.xml.Node

object Multimedia {
  val IDENTIFIER_TERM =  DcTerm.identifier
  val FORMAT_TERM =  DcTerm.format
  val EXTENSION_PATTERN = raw"\.[\d\w\-_]+".r
  val EXTENSION_MAP = Map(
    ".jpg" -> "image/jpeg",
    ".gif" -> "image/gif",
    ".png" -> "image/png"
  )

  /**
   * Find a format description for a piece of multimedia.
   * <p>
   * If there is an explicit format, use that.
   * If there is no explicit format, try the extension of the identifier term.
   * Otherwise assume some sort of image.
   *
   * @param metadata The metadata
   *
   * @return The multimedia mime type
   */
  def findMimeType(metadata: Map[Term, String]): String = {
    val format: String = metadata get FORMAT_TERM match {
      case Some(f: String) => f
      case None => {
        metadata get IDENTIFIER_TERM match {
          case Some(id: String) => {
            EXTENSION_PATTERN findFirstMatchIn id match {
              case Some(m: Regex.Match) => EXTENSION_MAP.getOrElse(m.matched.toLowerCase, "image/*")
              case None => "image/*"
            }
          }
          case None => "image/*"
        }
      }
    }
    MimeType.matchTerm(format) match {
      case Some(term) => term.canonical
      case None => format
    }
  }

  /**
   * Create a new multimedia instance
   *
   * @param location The location of the multimedia
   * @param metadata The multimedia metadata
   *
   * @return The
   */
  def create(location: URL, metadata: Map[Term, String]): Multimedia = new Multimedia(location, findMimeType(metadata), metadata)
}

/**
 * A description of some sort of multimedia instance.
 *
 * @author Doug Palmer &lt;Doug.Palmer@csiro.au&gt;
 *
 *         Copyright (c) 2015 CSIRO
 */
class Multimedia(
  val location: URL,
  val mediaType: String,
  val metadata: Map[Term, String]
  ) extends Cloneable {

  /**
   * Provide metadata as a map of string onto string using the term's simple name as a key
   *
   * @return The metadata map as strings
   */
  def metadataAsStrings = metadata map {case (key, value) => (key.simpleName, value) }

  /**
   * Move this multimedia instance to a new location.
   *
   * @param loc The new location
   *
   * @return A new multimedia instance with the new location
   */
  def move(loc: URL): Multimedia = new Multimedia(loc, this.mediaType, this.metadata)

  /**
   * Add a metadata term
   *
   * @param term The term
   * @param value The value
   *
   * @return A new multimedia instance with the metadata added
   */
  def addMetadata(term: Term, value: String) = new Multimedia(this.location, this.mediaType, this.metadata + (term -> value))


  override def clone(): Multimedia = super.clone().asInstanceOf[Multimedia]
}
