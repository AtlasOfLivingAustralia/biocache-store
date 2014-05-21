package au.org.ala.biocache.load

import au.org.ala.biocache.cmd.Tool
import org.slf4j.LoggerFactory
import au.org.ala.biocache.util.OptionParser
import java.io.File
import org.apache.commons.io.FilenameUtils

/**
 * A utility for thumbnailing images
 */
object Thumbnailer extends Tool {

  def cmd = "thumbnail"
  def desc = "Generate thumbnails for images"

  final val logger = LoggerFactory.getLogger("Thumbnailer")
  System.setProperty("com.sun.media.jai.disableMediaLib", "true")

  /**
   * Runnable for generating thumbnails
   */
  def main(args: Array[String]) {

    var directoryPath = ""
    var filePath = ""
    var force = false

    val parser = new OptionParser(help) {
      opt("f", "absolute-file-path", "File path to image to generate thumbnails for", {
        v: String => filePath = v
      })
      opt("d", "absolute-directory-path", "Directory path to recursively", {
        v: String => directoryPath = v
      })
      booleanOpt("fc", "force", "Force the regeneration of thumbnails", {
        v: Boolean => force = v
      })
    }
    if (parser.parse(args)) {
      if (filePath != "") {
        generateAllSizes(new File(filePath), force)
      }
      if (directoryPath != "") {
        recursivelyGenerateThumbnails(new File(directoryPath), force)
      }
    }
  }

  /**
   * Recursively crawl directories and generate thumbnails
   */
  def recursivelyGenerateThumbnails(directory: File, force:Boolean = false) {
    //dont generate thumbnails for thumbnails
    logger.info("Starting with directory: " + directory.getAbsolutePath)
    if (directory.isDirectory) {
      var children = directory.list
      if (children == null) {
        children = Array[String]()
      }
      logger.info("Recursive Dir: " + directory.getName + ", size of subDirs: " + children.length);
      for (i <- 0 until children.length) {
        recursivelyGenerateThumbnails(new File(directory, children(i)), force)
      }
    } else {
      //generate a thumbnail if this is an image
      if (MediaStore.isValidImageURL(directory.getAbsolutePath)) {
        generateAllSizes(directory, force)
      }
    }
  }

  /**
   * Generate thumbnails of all sizes
   */
  def generateAllSizes(source: File, force:Boolean = false) {
    val fileName = source.getName
    if (!fileName.contains(THUMB.suffix) && !fileName.contains(SMALL.suffix) && !fileName.contains(LARGE.suffix)) {
      generateThumbnail(source, THUMB, force)
      generateThumbnail(source, SMALL, force)
      generateThumbnail(source, LARGE, force)
    }
  }

  /**
   * Generate an image of the specified size.
   */
  def generateThumbnail(source: File, imageSize: ImageSize, force:Boolean = false) {
    val extension = FilenameUtils.getExtension(source.getAbsolutePath)
    val targetFilePath = source.getAbsolutePath.replace("." + extension, imageSize.suffix + "." + extension)
    val target = new File(targetFilePath)
    if(!target.exists() || force) {
      generateThumbnailToSize(source, target, imageSize.size)
    }
  }

  /**
   * Generata thumbanail to the specified file.
   */
  def generateThumbnailToSize(source: File, target: File, thumbnailSize: Float, force:Boolean = false) {
    val t = new ThumbnailableImage(source)
    t.writeThumbnailToFile(target, thumbnailSize)
  }
}
