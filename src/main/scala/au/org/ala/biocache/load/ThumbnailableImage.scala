package au.org.ala.biocache.load

import java.io.{FileOutputStream, File}
import com.sun.media.jai.codec.FileSeekableStream
import javax.media.jai.JAI
import java.awt.image.BufferedImage
import java.awt.Image
import javax.imageio.ImageIO
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

/**
 * An image that can be thumbnailed
 */
class ThumbnailableImage(imageFile: File) {

  final val logger = LoggerFactory.getLogger("ThumbnailableImage")
  final val fss = new FileSeekableStream(imageFile)
  final val originalImage = JAI.create("stream", fss)

  /**
   * Write a thumbnail to file
   */
  def writeThumbnailToFile(newThumbnailFile: File, edgeLength: Float) {
    try{
      val height = originalImage.getHeight
      val width = originalImage.getWidth
      val renderedImage = originalImage.createSnapshot.asInstanceOf[javax.media.jai.RenderedOp]
      if (!(height < edgeLength && width < edgeLength)) {
        val denom = {
          if (height > width) height
          else width
        }
        val modifier = edgeLength / denom
        val w = (width * modifier).toInt
        val h = (height * modifier).toInt
        val i = renderedImage.getAsBufferedImage.getScaledInstance(w, h, Image.SCALE_SMOOTH)
        val bufferedImage = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB)
        val g = bufferedImage.createGraphics
        g.drawImage(i, null, null)
        g.dispose
        i.flush
        val modifiedImage = JAI.create("awtImage", bufferedImage.asInstanceOf[Image])
        val fOut = new FileOutputStream(newThumbnailFile)
        ImageIO.write(modifiedImage, "jpg", fOut)
        fOut.flush
        fOut.close
      } else {
        FileUtils.copyFile(imageFile, newThumbnailFile)
      }
    } catch {
      //NC:2013-07-05: Need to catch this exception in case there is an issue with one of the images.
      case e: Exception => logger.error("Unable to generate thumbnail for " + imageFile.getAbsoluteFile , e)
    }
  }
}
