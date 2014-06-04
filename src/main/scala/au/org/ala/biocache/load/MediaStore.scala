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
import au.org.ala.biocache.util.{HttpUtil, Json, OptionParser}
import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.entity.mime.{HttpMultipartMode, MultipartEntity}
import org.apache.http.entity.mime.content.{StringBody, FileBody}
import org.apache.http.client.methods.HttpPost
import scala.io.Source
import org.apache.commons.lang3.StringUtils
import scala.util.parsing.json.JSON
import com.jayway.jsonpath.JsonPath
import net.minidev.json.JSONArray

/**
 * Trait for Media stores to implement.
 */
trait MediaStore {

  //Regular expression used to parse an image URL - adapted from
  //http://stackoverflow.com/questions/169625/regex-to-check-if-valid-url-that-ends-in-jpg-png-or-gif#169656
  lazy val imageParser = """^(https?://[^\'"<>]+?\.(jpg|jpeg|gif|png))$""".r
  lazy val soundParser = """^(https?://[^\'"<>]+?\.(?:wav|mp3|ogg|flac))$""".r
  lazy val videoParser = """^(https?://[^\'"<>]+?\.(?:wmv|mp4|mpg|avi|mov))$""".r

  val imageExtension = Array(".jpg", ".gif", ".png", ".jpeg", "imgType=jpeg")
  val soundExtension = Array(".wav", ".mp3", ".ogg", ".flac")
  val videoExtension = Array(".wmv", ".mp4", ".mpg", ".avi", ".mov")

  val logger = LoggerFactory.getLogger("MediaStore")

  def isValidImageURL(url: String): Boolean =
    !imageParser.unapplySeq(url.trim.toLowerCase).isEmpty || isStoredMedia(imageExtension, url)

  def isValidSoundURL(url: String): Boolean =
    !soundParser.unapplySeq(url.trim.toLowerCase).isEmpty || isStoredMedia(soundExtension, url)

  def isValidVideoURL(url: String): Boolean =
    !videoParser.unapplySeq(url.trim.toLowerCase).isEmpty || isStoredMedia(videoExtension, url)

  def isMediaFile(file:File): Boolean = {
    val name = file.getAbsolutePath()
    endsWithOneOf(imageExtension,name) || endsWithOneOf(soundExtension,name) || endsWithOneOf(videoExtension,name)
  }

  def endsWithOneOf(acceptedExtensions: Array[String], url: String): Boolean =
    !(acceptedExtensions collectFirst { case x if url.endsWith(x) => x } isEmpty)


  protected def extractFileName(urlToMedia: String): String = {
    if (urlToMedia.contains("fileName=")) {
      //HACK for CS URLs which dont make for nice file names
      urlToMedia.substring(urlToMedia.indexOf("fileName=") + "fileName=".length).replace(" ", "_")
    } else if (urlToMedia.contains("?id=") && urlToMedia.contains("imgType=")) {
      // HACK for Morphbank URLs which don't make nice file names
      urlToMedia.substring(urlToMedia.lastIndexOf("/") + 1).replace("?id=", "").replace("&imgType=", ".")
    } else if (urlToMedia.lastIndexOf("/") == urlToMedia.length - 1) {
      "raw"
    } else {
      urlToMedia.substring(urlToMedia.lastIndexOf("/") + 1).replace(" ", "_")
    }
  }

  /**
   * Test to see if the supplied file is already stored in the media store.
   * Returns boolean indicating if stored, and string indicating location if stored.
   *
   * @param uuid
   * @param resourceUID
   * @param urlToMedia
   * @return
   */
  def alreadyStored(uuid: String, resourceUID: String, urlToMedia: String): (String, Boolean)

  /**
   * Checks to see if the supplied media file is accessible on the file system
   * or over http.
   *
   * @param urlString
   * @return true if successful
   */
  def isAccessible(urlString: String): Boolean = {

    if(StringUtils.isBlank(urlString)){
      return false
    }

    val urlToTest = if (urlString.startsWith(Config.mediaFileStore)){
      "file://" + urlString
    } else {
      urlString
    }

    var in: java.io.InputStream = null

    try {
      val url = new java.net.URL(urlToTest.replaceAll(" ", "%20"))
      in = url.openStream
      true
    } catch {
      case e:Exception => {
        logger.debug("File with URI '" + urlString + "' is not accessible: " + e.getMessage())
        false
      }
    } finally {
      if (in != null) {
        in.close()
      }
    }
  }

  /**
   * Save the supplied media file returning a handle for retrieving
   * the media file.
   *
   * @param uuid
   * @param resourceUID
   * @param urlToMedia
   * @return
   */
  def save(uuid: String, resourceUID: String, urlToMedia: String): Option[String]

  def alternativeFormats(filePath: String): Array[String]

  def convertPathsToUrls(fullRecord: FullRecord, baseUrlPath: String) : Unit

  def convertPathToUrl(str: String, baseUrlPath: String) :String

  def convertPathToUrl(str: String):String
  
  protected def isStoredMedia(acceptedExtensions: Array[String], url: String): Boolean
}

/**
 * A media store than provides the integration with the image service.
 *
 * https://code.google.com/p/ala-images/
 */
object RemoteMediaStore extends MediaStore {

  override val logger = LoggerFactory.getLogger("RemoteMediaStore")

  /**
   * Calls the remote service to retrieve an identifier for the media.
   *
   * @param uuid
   * @param resourceUID
   * @param urlToMedia
   * @return
   */
  def alreadyStored(uuid: String, resourceUID: String, urlToMedia: String): (String, Boolean) = {

    //check image store for the supplied resourceUID/UUID/Filename combination
    //http://images.ala.org.au/ws/findImagesByOriginalFilename?
    // filenames=http://biocache.ala.org.au/biocache-media/dr836/29790/1b6c48ab-0c11-4d2e-835e-85d016f335eb/PWCnSmwl.jpeg

    val jsonToPost = Json.toJSON(Map("filenames" -> Array(constructFileID(resourceUID, uuid, urlToMedia))))

    logger.info(jsonToPost)

    val (code, body) = HttpUtil.postBody(
      Config.remoteMediaStoreUrl + "/ws/findImagesByOriginalFilename",
      "application/json",
      jsonToPost
    )

    if(code == 200){
      try {
        val jsonPath = JsonPath.compile("$..imageId")
        val idArray = jsonPath.read(body).asInstanceOf[JSONArray]
        if(idArray.isEmpty) {
          ("", false)
        } else if (idArray.size() == 0) {
          ("", false)
        } else {
          val imageId = idArray.get(0)
          logger.info(s"Image $urlToMedia already stored here: " + Config.remoteMediaStoreUrl + imageId )
          (imageId.toString(), true)
        }
      } catch {
        case e:Exception => {
          logger.debug(e.getMessage, e)
          ("", false)
        }
      }
    } else {
      ("", false)
    }
  }

  protected def constructFileID(resourceUID: String, uuid: String, urlToMedia: String) =
    resourceUID + "||" + uuid + "||" + extractFileName(urlToMedia)

  /**
   * Save the supplied media to the remote store.
   *
   * @param uuid
   * @param resourceUID
   * @param urlToMedia
   * @return
   */
  def save(uuid: String, resourceUID: String, urlToMedia: String): Option[String] = {

    val tmpFile = new File(Config.tmpWorkDir + File.separator + constructFileID(resourceUID, uuid, urlToMedia))
    val url = new java.net.URL(urlToMedia.replaceAll(" ", "%20"))
    val in = url.openStream
    val out = new FileOutputStream(tmpFile)
    val buffer: Array[Byte] = new Array[Byte](1024)
    var numRead = 0
    while ( {
      numRead = in.read(buffer)
      numRead != -1
    }) {
      out.write(buffer, 0, numRead)
      out.flush
    }
    in.close()
    out.close()
    try {
      uploadImage(uuid, resourceUID, urlToMedia, tmpFile)
    } finally {
      FileUtils.forceDelete(tmpFile)
    }
  }

  /**
   * Uploads an image to the service and returns a ID for the image.
   *
   * @param fileToUpload
   * @return
   */
  private def uploadImage(uuid:String, resourceUID:String, urlToMedia:String, fileToUpload:File) : Option[String] = {
    //upload an image
    val httpClient = new DefaultHttpClient()
    val entity = new MultipartEntity(HttpMultipartMode.BROWSER_COMPATIBLE)
    val fileBody = new FileBody(fileToUpload, "image/jpeg")
    entity.addPart("image", fileBody)
    entity.addPart("metadata",
      new StringBody(
        Json.toJSON(
          Map(
            "occurrenceId" -> uuid,
            "dataResourceUid" -> resourceUID,
            "originalFileName" -> extractFileName(urlToMedia),
            "fullOriginalUrl" -> urlToMedia
          )
        )
      )
    )

    val httpPost = new HttpPost(Config.remoteMediaStoreUrl + "/ws/uploadImage")
    httpPost.setEntity(entity)
    val response = httpClient.execute(httpPost)
    val result = response.getStatusLine()
    val responseBody = Source.fromInputStream(response.getEntity().getContent()).mkString
    logger.info("Image service response code: " + result.getStatusCode)

    val map = Json.toMap(responseBody)
    logger.info("Image ID: " + map.getOrElse("imageId", ""))
    map.get("imageId") match {
      case Some(o) => Some(o.toString())
      case None => None
    }
  }

  def alternativeFormats(filePath: String): Array[String]={
    logger.error("[alternativeFormats] Not implemented yet")
    Array[String]()
  }

  protected def isStoredMedia(acceptedExtensions: Array[String], url: String): Boolean =
    url.startsWith(Config.remoteMediaStoreUrl)

  def convertPathsToUrls(fullRecord: FullRecord, baseUrlPath: String) =
    logger.error("[convertPathsToUrls] Not implemented yet")

  def convertPathToUrl(str: String, baseUrlPath: String) = Config.remoteMediaStoreUrl + str

  def convertPathToUrl(str: String) = Config.remoteMediaStoreUrl + str
}

/**
 * A file store for media files that uses the local filesystem.
 *
 * @author Dave Martin
 */
object LocalMediaStore extends MediaStore {

  override val logger = LoggerFactory.getLogger("LocalMediaStore")

  /** Some unix filesystems has a limit of 32k files per directory */
  val limit = 32000

  /**
   * Returns true if the supplied url looks like a path to an image stored in the media
   * file store and ends with one of the accepted extensions.
   *
   * @param acceptedExtensions
   * @param url
   * @return
   */
  protected def isStoredMedia(acceptedExtensions: Array[String], url: String): Boolean = {
    ( url.startsWith(Config.mediaFileStore) || url.startsWith("file:///" + Config.mediaFileStore) ) && endsWithOneOf(acceptedExtensions, url.toLowerCase)
  }

  def convertPathsToUrls(fullRecord: FullRecord, baseUrlPath: String) = if (fullRecord.occurrence.images != null) {
    fullRecord.occurrence.images = fullRecord.occurrence.images.map(x => convertPathToUrl(x, baseUrlPath))
  }

  def convertPathToUrl(str: String, baseUrlPath: String) = str.replaceAll(Config.mediaFileStore, baseUrlPath)

  def convertPathToUrl(str: String) = str.replaceAll(Config.mediaFileStore, Config.mediaBaseUrl)

  def alreadyStored(uuid: String, resourceUID: String, urlToMedia: String): (String, Boolean) = {
    val path = createFilePath(uuid, resourceUID, urlToMedia)
    (path, (new File(path)).exists)
  }

  /**
   * Create a file path for this UUID and resourceUID
   */
  private def createFilePath(uuid: String, resourceUID: String, urlToMedia: String): String = {
    val subdirectory = (uuid.hashCode % limit).abs

    val absoluteDirectoryPath = Config.mediaFileStore +
      File.separator + resourceUID +
      File.separator + subdirectory +
      File.separator + uuid

    val directory = new File(absoluteDirectoryPath)
    if (!directory.exists) FileUtils.forceMkdir(directory)

    directory.getAbsolutePath + File.separator + extractFileName(urlToMedia)
  }

  /**
   * Saves the file to local filesystem and returns the file path where the file is stored.
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
  def suffix = "__thumb"
  def size = 100f
}

object SMALL extends ImageSize {
  def suffix = "__small"
  def size = 314f
}

object LARGE extends ImageSize {
  def suffix = "__large"
  def size = 650f
}