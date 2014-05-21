package au.org.ala.biocache.load

import org.slf4j.LoggerFactory
import org.apache.commons.io.{FilenameUtils, FileUtils}
import java.io._
import javax.imageio.ImageIO
import javax.media.jai.JAI
import com.sun.media.jai.codec.FileSeekableStream
import java.awt.Image
import java.awt.image.BufferedImage
import au.org.ala.biocache.model.FullRecord
import au.org.ala.biocache.util.OptionParser
import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool

/**
 * A file store for media files.
 *
 * @author Dave Martin
 */
object MediaStore {

  val logger = LoggerFactory.getLogger("MediaStore")

  //Regular expression used to parse an image URL - adapted from
  //http://stackoverflow.com/questions/169625/regex-to-check-if-valid-url-that-ends-in-jpg-png-or-gif#169656
  lazy val imageParser = """^(https?://[^\'"<>]+?\.(jpg|jpeg|gif|png))$""".r
  lazy val soundParser = """^(https?://[^\'"<>]+?\.(?:wav|mp3|ogg|flac))$""".r
  lazy val videoParser = """^(https?://[^\'"<>]+?\.(?:wmv|mp4|mpg|avi|mov))$""".r

  val imageExtension = Array(".jpg", ".gif", ".png", ".jpeg", "imgType=jpeg")
  val soundExtension = Array(".wav", ".mp3", ".ogg", ".flac")
  val videoExtension = Array(".wmv", ".mp4", ".mpg", ".avi", ".mov")

  val limit = 32000

  def doesFileExist(urlString: String): Boolean = {

    val urlToTest = if (urlString.startsWith(Config.mediaFileStore)) "file://" + urlString else urlString
    var in: java.io.InputStream = null

    try {
      val url = new java.net.URL(urlToTest.replaceAll(" ", "%20"))
      in = url.openStream
      true
    }
    catch {
      case _:Exception => false
    } finally {
      if (in != null) {
        in.close()
      }
    }
  }

  def isValidImageURL(url: String): Boolean = {
    !imageParser.unapplySeq(url.trim.toLowerCase).isEmpty || isStoredMedia(imageExtension, url)
  }

  def isValidSoundURL(url: String): Boolean = {
    !soundParser.unapplySeq(url.trim.toLowerCase).isEmpty || isStoredMedia(soundExtension, url)
  }

  def isValidVideoURL(url: String): Boolean = {
    !videoParser.unapplySeq(url.trim.toLowerCase).isEmpty || isStoredMedia(videoExtension, url)
  }
  
  def isMediaFile(file:File): Boolean ={
    val name = file.getAbsolutePath()
    endsWithOneOf(imageExtension,name)||endsWithOneOf(soundExtension,name)||endsWithOneOf(videoExtension,name)
  }

  def isStoredMedia(acceptedExtensions: Array[String], url: String): Boolean = {
    ( url.startsWith(Config.mediaFileStore) || url.startsWith("file:///" + Config.mediaFileStore) ) && endsWithOneOf(acceptedExtensions, url.toLowerCase)
  }

  def endsWithOneOf(acceptedExtensions: Array[String], url: String): Boolean = {
    !(acceptedExtensions collectFirst {
      case x if url.endsWith(x) => x
    } isEmpty)
  }

  def convertPathsToUrls(fullRecord: FullRecord, baseUrlPath: String) {
    if (fullRecord.occurrence.images != null) {
      fullRecord.occurrence.images = fullRecord.occurrence.images.map(x => convertPathToUrl(x, baseUrlPath))
    }
  }

  def convertPathToUrl(str: String, baseUrlPath: String) = str.replaceAll(Config.mediaFileStore, baseUrlPath)

  def convertPathToUrl(str: String) = str.replaceAll(Config.mediaFileStore, Config.mediaBaseUrl)

  def exists(uuid: String, resourceUID: String, urlToMedia: String): (String, Boolean) = {
    val path = createFilePath(uuid, resourceUID, urlToMedia)
    (path, (new File(path)).exists)
  }

  /**
   * Create a file path for this UUID and resourceUID
   */
  def createFilePath(uuid: String, resourceUID: String, urlToMedia: String): String = {
    val subdirectory = (uuid.hashCode % limit).abs

    val absoluteDirectoryPath = Config.mediaFileStore +
      File.separator + resourceUID +
      File.separator + subdirectory +
      File.separator + uuid

    val directory = new File(absoluteDirectoryPath)
    if (!directory.exists) FileUtils.forceMkdir(directory)

    val fileName = {
      if (urlToMedia.contains("fileName=")) {
        //HACK for CS URLs which dont make for nice file names
        urlToMedia.substring(urlToMedia.indexOf("fileName=") + "fileName=".length).replace(" ", "_")
      } else if (urlToMedia.contains("?id=") && urlToMedia.contains("imgType=")) {
        // HACK for Morphbank URLs which don't make nice filenames
        urlToMedia.substring(urlToMedia.lastIndexOf("/") + 1).replace("?id=", "").replace("&imgType=", ".")
      } else if (urlToMedia.lastIndexOf("/") == urlToMedia.length - 1) {
        "raw"
      } else {
        urlToMedia.substring(urlToMedia.lastIndexOf("/") + 1).replace(" ", "_")
      }
    }
    directory.getAbsolutePath + File.separator + fileName
  }

  /**
   * Returns the file path
   */
  def save(uuid: String, resourceUID: String, urlToMedia: String): Option[String] = {
    //handle the situation where the urlToMedia does not exits -

    var in: java.io.InputStream = null
    var out: java.io.FileOutputStream = null

    try {
      val fullPath = createFilePath(uuid, resourceUID, urlToMedia)
      val file = new File(fullPath)
      if (!file.exists() || file.length() == 0) {
        val url = new java.net.URL(urlToMedia.replaceAll(" ", "%20"))
        in = url.openStream
        out = new FileOutputStream(file)
        val buffer: Array[Byte] = new Array[Byte](1024)
        var numRead = 0
        while ( {
          numRead = in.read(buffer)
          numRead != -1
        }) {
          out.write(buffer, 0, numRead)
          out.flush
        }
        logger.info("File saved to: " + fullPath)
        //is this file an image???
        if (isValidImageURL(urlToMedia)) {
          Thumbnailer.generateAllSizes(new File(fullPath))
        } else {
          logger.warn("Invalid media file. Not generating derivatives for: " + fullPath)
        }
      } else {
        logger.info("File previously saved to: " + fullPath)
        if (isValidImageURL(urlToMedia)) {
          Thumbnailer.generateAllSizes(new File(fullPath))
        } else {
          logger.warn("Invalid media file. Not generating derivatives for: " + fullPath)
        }
      }
      //store the media
      Some(fullPath)
    } catch {
      case e: Exception => logger.warn("Unable to load media " + urlToMedia + ". " + e.getMessage); None
    } finally {
      if (in != null) {
        in.close()
      }

      if (out != null) {
        out.close()
      }
    }
  }

  def alternativeFormats(filePath: String): Array[String] = {
    val file = new File(filePath)
    file.exists match {
      case true => {
        val filenames = file.getParentFile.list(new SameNameDifferentExtensionFilter(filePath))
        filenames.map(f => file.getParent + File.separator + f)
      }
      case false => Array()
    }
  }
}

class SameNameDifferentExtensionFilter(name: String) extends FilenameFilter {
  val nameToMatch = FilenameUtils.removeExtension(FilenameUtils.getName(name)).toLowerCase

  def accept(dir: File, name: String) = FilenameUtils.removeExtension(name.toLowerCase) == nameToMatch
}

trait ImageSize {
  def suffix: String

  def size: Float
}

object THUMB extends ImageSize {
  def suffix = "__thumb";

  def size = 100f;
}

object SMALL extends ImageSize {
  def suffix = "__small";

  def size = 314f;
}

object LARGE extends ImageSize {
  def suffix = "__large";

  def size = 650f;
}



