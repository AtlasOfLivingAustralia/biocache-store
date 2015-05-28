package au.org.ala.biocache.load

import java.net.{URI, URL}
import java.security.MessageDigest

import org.apache.commons.codec.digest.DigestUtils
import org.apache.http.NameValuePair
import org.apache.http.client.utils.URLEncodedUtils
import org.restlet.engine.adapter.Call
import org.slf4j.LoggerFactory
import org.apache.commons.io.{FilenameUtils, FileUtils}
import java.io._
import javax.imageio.ImageIO
import javax.media.jai.JAI
import com.sun.media.jai.codec.FileSeekableStream
import java.awt.Image
import java.awt.image.BufferedImage
import au.org.ala.biocache.model.{Multimedia, FullRecord}
import au.org.ala.biocache.util.{ HttpUtil, Json, OptionParser}
import au.org.ala.biocache.{Store, Config}
import au.org.ala.biocache.cmd.Tool
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.entity.mime.{HttpMultipartMode, MultipartEntity}
import org.apache.http.entity.mime.content.{StringBody, FileBody}
import org.apache.http.client.methods.HttpPost
import scala.collection.mutable
import scala.io.Source
import org.apache.commons.lang3.StringUtils
import scala.util.parsing.json.JSON
import com.jayway.jsonpath.JsonPath
import net.minidev.json.JSONArray
import java.util
import javax.activation.MimeType

/**
 * Trait for Media stores to implement.
 */
trait MediaStore {

  val logger = LoggerFactory.getLogger("MediaStore")

  //Regular expression used to parse an image URL - adapted from
  //http://stackoverflow.com/questions/169625/regex-to-check-if-valid-url-that-ends-in-jpg-png-or-gif#169656
  //Extended to allow query parameters after the path and ftp as well as http access
  lazy val imageParser = """^((?:http|ftp|file)s?://[^\'"<>]+?\.(jpg|jpeg|gif|png)(\?.+)?)$""".r
  lazy val soundParser = """^((?:http|ftp|file)s?://[^\'"<>]+?\.(?:wav|mp3|ogg|flac)(\?.+)?)$""".r
  lazy val videoParser = """^((?:http|ftp|file)s?://[^\'"<>]+?\.(?:wmv|mp4|mpg|avi|mov)(\?.+)?)$""".r

  val imageExtension = Array(".jpg", ".gif", ".png", ".jpeg", "imgType=jpeg")
  val soundExtension = Array(".wav", ".mp3", ".ogg", ".flac")
  val videoExtension = Array(".wmv", ".mp4", ".mpg", ".avi", ".mov")

  def isValidImageURL(url: String) = !imageParser.unapplySeq(url.trim.toLowerCase).isEmpty
  def isValidSoundURL(url: String) = !soundParser.unapplySeq(url.trim.toLowerCase).isEmpty
  def isValidVideoURL(url: String) = !videoParser.unapplySeq(url.trim.toLowerCase).isEmpty

  def isValidImage(filename: String) = endsWithOneOf(imageExtension, filename) || !imageParser.findAllMatchIn(filename).isEmpty
  def isValidSound(filename: String) = endsWithOneOf(soundExtension, filename) || !soundParser.findAllMatchIn(filename).isEmpty
  def isValidVideo(filename: String) = endsWithOneOf(videoExtension, filename) || !videoParser.findAllMatchIn(filename).isEmpty

  def getImageFormats(filenameOrID:String) : java.util.Map[String, String]

  def isMediaFile(file:File) : Boolean = {
    val name = file.getAbsolutePath()
    endsWithOneOf(imageExtension,name) || endsWithOneOf(soundExtension,name) || endsWithOneOf(videoExtension,name)
  }

  def endsWithOneOf(acceptedExtensions: Array[String], url: String): Boolean =
    !(acceptedExtensions collectFirst { case x if url.toLowerCase().endsWith(x) => x } isEmpty)

  protected def extractSimpleFileName(urlToMedia: String): String = {
    val base = urlToMedia.substring(urlToMedia.lastIndexOf("/") + 1).trim
    val queryPart = base.lastIndexOf("?")
    if (queryPart < 0) base else base.substring(0, queryPart).trim
  }

  protected def extractFileName(urlToMedia: String): String = if (urlToMedia.contains("fileName=")) {
    //HACK for CS URLs which dont make for nice file names
    urlToMedia.substring(urlToMedia.indexOf("fileName=") + "fileName=".length).replace(" ", "_")
  } else if (urlToMedia.contains("?id=") && urlToMedia.contains("imgType=")) {
    // HACK for Morphbank URLs which don't make nice file names
    extractSimpleFileName(urlToMedia).replace("?id=", "").replace("&imgType=", ".")
  } else if (urlToMedia.lastIndexOf("/") == urlToMedia.length - 1) {
    "raw"
  } else if(Config.hashImageFileNames) {
    val md = MessageDigest.getInstance("MD5")
    val fileName = extractSimpleFileName(urlToMedia)
    val extension = FilenameUtils.getExtension(fileName)
    if(extension !=null && extension !="") {
      DigestUtils.md5Hex(fileName) + "." + extension
    } else {
      DigestUtils.md5Hex(fileName)
    }
  } else {
    extractSimpleFileName(urlToMedia).replace(" ", "_")
  }

  /**
   * Test to see if the supplied file is already stored in the media store.
   * Returns tuple with:
   *
   * boolean - true if stored
   * String - original file name
   * String - identifier or filesystem path to where the media is stored.
   *
   * @param uuid
   * @param resourceUID
   * @param urlToMedia
   * @return
   */
  def alreadyStored(uuid: String, resourceUID: String, urlToMedia: String) : (Boolean, String, String)

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
   * @param uuid Media uuid
   * @param resourceUID Resource associated with the media
   * @param urlToMedia The media source
   * @param media Optional multimedia instance containing additional metadata
   *
   * @return
   */
  def save(uuid: String, resourceUID: String, urlToMedia: String, media: Option[Multimedia]) : Option[(String, String)]

  def getSoundFormats(filePath: String): java.util.Map[String, String]

  def convertPathsToUrls(fullRecord: FullRecord, baseUrlPath: String) : Unit

  def convertPathToUrl(str: String, baseUrlPath: String) : String

  def convertPathToUrl(str: String) : String
}

/**
 * A media store than provides the integration with the image service.
 *
 * https://code.google.com/p/ala-images/
 */
object RemoteMediaStore extends MediaStore {

  import scala.collection.JavaConversions._
  override val logger = LoggerFactory.getLogger("RemoteMediaStore")

  def getImageFormats(imageId:String) : java.util.Map[String, String] = {
    val map = new util.HashMap[String,String]
    map.put("thumb", Config.remoteMediaStoreUrl + "/image/proxyImageThumbnail?imageId=" + imageId)
    map.put("small", Config.remoteMediaStoreUrl + "/image/proxyImageThumbnail?imageId=" + imageId)
    map.put("large", Config.remoteMediaStoreUrl + "/image/proxyImageThumbnailLarge?imageId=" + imageId)
    map.put("raw",   Config.remoteMediaStoreUrl + "/image/proxyImage?imageId=" + imageId)
    map
  }

  /**
   * Calls the remote service to retrieve an identifier for the media.
   *
   * @param uuid
   * @param resourceUID
   * @param urlToMedia
   * @return
   */
  def alreadyStored(uuid: String, resourceUID: String, urlToMedia: String): (Boolean, String, String) = {

    //check image store for the supplied resourceUID/UUID/Filename combination
    //http://images.ala.org.au/ws/findImagesByOriginalFilename?
    // filenames=http://biocache.ala.org.au/biocache-media/dr836/29790/1b6c48ab-0c11-4d2e-835e-85d016f335eb/PWCnSmwl.jpeg
    val jsonToPost = Json.toJSON(Map("filenames" -> Array(constructFileID(resourceUID, uuid, urlToMedia))))

    logger.debug(jsonToPost)

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
          (false, "", "")
        } else if (idArray.size() == 0) {
          (false, "", "")
        } else {
          val imageId = idArray.get(0)
          logger.info(s"Image $urlToMedia already stored here: " + Config.remoteMediaStoreUrl + s"/image/proxyImage?imageId=$imageId")
          (true, extractFileName(urlToMedia), imageId.toString())
        }
      } catch {
        case e:Exception => {
          logger.debug(e.getMessage, e)
          (false, "", "")
        }
      }
    } else {
      (false, "", "")
    }
  }

  /**
   * Construct the ID used by the image store. Changing this will cause duplication
   * problems with existing image resources as this is used to check uniqueness.
   *
   * @param resourceUID
   * @param uuid
   * @param urlToMedia
   * @return
   */
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
  def save(uuid: String, resourceUID: String, urlToMedia: String, media: Option[Multimedia]): Option[(String, String)] = {

    //is the supplied URL an image service URL ?? If so extract imageID and return.....
    if(urlToMedia.startsWith(Config.remoteMediaStoreUrl)){
      logger.info("Remote media store URL recognised: " + urlToMedia)
      //  http://images.ala.org.au/image/proxyImageThumbnailLarge?imageId=119d85b5-76cb-4d1d-af30-e141706be8bf
      val uri = new URI(urlToMedia)
      val params:java.util.List[NameValuePair] = URLEncodedUtils.parse(uri, "UTF-8")
      val param = params.get(0)
      if(param.getName.toLowerCase == "imageid"){
        val imageId = param.getValue()
        val imageMetadataUrl = new URL(Config.remoteMediaStoreUrl + "/ws/getImageInfo?id=" + imageId)
        val response = Source.fromURL(imageMetadataUrl).getLines().mkString
        val metadata = Json.toMap(response)
        return Some(metadata.getOrElse("originalFileName", "").toString, param.getValue())
      }
    }

    downloadToTmpFile(resourceUID, uuid, urlToMedia) match {
      case Some(tmpFile) => {
        try {
          val imageId = uploadImage(uuid, resourceUID, urlToMedia, tmpFile, media)
          if(imageId.isDefined){
            Some((extractFileName(urlToMedia), imageId.getOrElse("")))
          } else {
            None
          }
        } finally {
          FileUtils.forceDelete(tmpFile)
        }
      }
      case None => None
    }
  }

  private def downloadToTmpFile(resourceUID: String, uuid: String, urlToMedia: String): Option[File] = try {
    val tmpFile = new File(Config.tmpWorkDir + File.separator + constructFileID(resourceUID, uuid, urlToMedia))
    val url = new java.net.URL(urlToMedia.replaceAll(" ", "%20"))
    val in = url.openStream
    val out = new FileOutputStream(tmpFile)
    val buffer: Array[Byte] = new Array[Byte](1024)
    var numRead = 0
    while ( {
      numRead = in.read(buffer);
      numRead != -1
    }) {
      out.write(buffer, 0, numRead)
      out.flush
    }
    in.close()
    out.close()
    Some(tmpFile)
  } catch {
    case e:Exception => {
      logger.error("Problem downloading media. URL:" + urlToMedia)
      logger.debug(e.getMessage, e)
      None
    }
  }

  /**
   * Uploads an image to the service and returns a ID for the image.
   *
   * @param fileToUpload
   * @return
   */
  private def uploadImage(uuid:String, resourceUID:String, urlToMedia:String, fileToUpload:File, media: Option[Multimedia]) : Option[String] = {
    //upload an image
    val httpClient = new DefaultHttpClient()
    val entity = new MultipartEntity(HttpMultipartMode.BROWSER_COMPATIBLE)
    if(!fileToUpload.exists()){
      logger.error("File to upload does not exist or can not be read. " + fileToUpload.getAbsolutePath)
      return None
    } else {
      logger.debug("File to upload: " + fileToUpload.getAbsolutePath + ", size:"  + fileToUpload.length())
    }
    val fileBody = new FileBody(fileToUpload, "image/jpeg")
    val metadata = mutable.Map(
      "occurrenceId" -> uuid,
      "dataResourceUid" -> resourceUID,
      "originalFileName" -> extractFileName(urlToMedia),
      "fullOriginalUrl" -> urlToMedia
    )

    if (media isDefined)
      metadata ++= media.get.metadataAsStrings

    entity.addPart("image", fileBody)
    entity.addPart("metadata",
      new StringBody(
        Json.toJSON(
          metadata
        )
      )
    )

    val httpPost = new HttpPost(Config.remoteMediaStoreUrl + "/ws/uploadImage")
    httpPost.setEntity(entity)
    val response = httpClient.execute(httpPost)
    val result = response.getStatusLine()
    val responseBody = Source.fromInputStream(response.getEntity().getContent()).mkString
    logger.debug("Image service response code: " + result.getStatusCode)
    val map = Json.toMap(responseBody)
    logger.debug("Image ID: " + map.getOrElse("imageId", ""))
    map.get("imageId") match {
      case Some(o) => Some(o.toString())
      case None => {
        logger.warn(s"Unable to persist image. Response code $result.getStatusCode.  Image service response body: $responseBody")
        None
      }
    }
  }

  def getSoundFormats(mediaID: String): java.util.Map[String,String]= {
    val formats = new java.util.HashMap[String, String]()
    formats.put("audio/mpeg", Config.remoteMediaStoreUrl +  "/image/proxyImage?imageId=" + mediaID )
    formats
  }

  def convertPathsToUrls(fullRecord: FullRecord, baseUrlPath: String) = if (fullRecord.occurrence.images != null) {
    fullRecord.occurrence.images = fullRecord.occurrence.images.map(x => convertPathToUrl(x, baseUrlPath))
  }

  def convertPathToUrl(str: String, baseUrlPath: String) = Config.remoteMediaStoreUrl + "/image/proxyImageThumbnail?imageId=" + str

  def convertPathToUrl(str: String) = Config.remoteMediaStoreUrl + "/image/proxyImageThumbnail?imageId=" + str
}

/**
 * A file store for media files that uses the local filesystem.
 *
 * @author Dave Martin
 */
object LocalMediaStore extends MediaStore {

  override val logger = LoggerFactory.getLogger("LocalMediaStore")

  import scala.collection.JavaConversions._

  /** Some unix filesystems has a limit of 32k files per directory */
  val limit = 32000

  def getImageFormats(fileName:String) : java.util.Map[String, String] = {
    val url = convertPathToUrl(fileName)
    val extension = url.substring(url.lastIndexOf("."))
    val map = new util.HashMap[String,String]
    map.put("thumb", url.replace(extension, "__thumb" + extension))
    map.put("small", url.replace(extension, "__small" + extension))
    map.put("large", url.replace(extension, "__large" + extension))
    map.put("raw", url)
    map
  }

  def convertPathsToUrls(fullRecord: FullRecord, baseUrlPath: String) = if (fullRecord.occurrence.images != null) {
    fullRecord.occurrence.images = fullRecord.occurrence.images.map(x => convertPathToUrl(x, baseUrlPath))
  }

  def convertPathToUrl(str: String, baseUrlPath: String) = str.replaceAll(Config.mediaFileStore, baseUrlPath)

  def convertPathToUrl(str: String) = str.replaceAll(Config.mediaFileStore, Config.mediaBaseUrl)

  def alreadyStored(uuid: String, resourceUID: String, urlToMedia: String) : (Boolean, String, String) = {
    val path = createFilePath(uuid, resourceUID, urlToMedia)
    (new File(path).exists, extractFileName(urlToMedia), path)
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
  def save(uuid: String, resourceUID: String, urlToMedia: String, media: Option[Multimedia]): Option[(String, String)] = {

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
      Some((extractFileName(urlToMedia), fullPath))
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

  val extensionToMimeTypes = Map(
    "mp3" -> "audio/mpeg",
    "ogg" ->  "audio/ogg"
  )

  def getSoundFormats(filePath: String): java.util.Map[String, String] = {
    val formats = new java.util.HashMap[String, String]()
    val file = new File(filePath)
    file.exists match {
      case true => {
        val filenames = file.getParentFile.list(new SameNameDifferentExtensionFilter(filePath))
        filenames.foreach(f => {
          val extension = FilenameUtils.getExtension(f).toLowerCase()
          val mimeType = extensionToMimeTypes.getOrElse(extension, "")
          formats.put(mimeType,convertPathToUrl(file.getParent + File.separator + f))
        })
      }
      case false => Array()
    }
    formats
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