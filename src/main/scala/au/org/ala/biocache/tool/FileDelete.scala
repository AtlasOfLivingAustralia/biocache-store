package au.org.ala.biocache.tool

import java.io.{FileOutputStream, File}
import scala.collection.mutable.ArrayBuffer
import au.org.ala.biocache.util.{StringHelper, FileHelper}
import net.sf.json.util.WebUtils
import java.util.Date
import org.apache.commons.io.FileUtils
import au.org.ala.biocache.Config

/**
 * A record deletor that takes a file of rowkeys to delete.
 */
class FileDelete(fileName: String, useUUID: Boolean  = false, fieldDelimiter:Char = '\t', hasHeader:Boolean = false) extends RecordDeletor {

  import FileHelper._

  //import the file constructs to allow lines to be easily iterated over
  override def deleteFromPersistent {
    logger.info("Using file name: " + fileName)
    var counter = 0
    getRemoteFile(fileName).readAsCSV(fieldDelimiter, '"', hdr => { List() },  (hdr, line) => {
      logger.info("Deleting ID : " + line(0))
      if (useUUID){
        occurrenceDAO.deleteByUuid(line(0), false, true)
      } else {
        occurrenceDAO.delete(line(0), false, true)
      }
      counter += 1
    })
    logger.info("Records deleted: " + counter)
  }

  override def deleteFromIndex {
    logger.info("deleteFromIndex - Using file name: " + fileName)
    var counter = 0
    val buf = new ArrayBuffer[String]
    val fieldName = if (useUUID) "id" else "row_key"
    getRemoteFile(fileName).foreachLine(line => {
      buf += line
      if (buf.size > 999) {
        //val query = "row_key:\"" + buf.mkString("\" OR row_key:\"") + "\""
        val query = fieldName + ":\"" + buf.mkString("\" OR "+ fieldName +":\"") + "\""
        indexer.removeByQuery(query)
        buf.clear()
      }
    })
    val query = fieldName + ":\"" + buf.mkString("\" OR "+ fieldName +":\"") + "\""
    indexer.removeByQuery(query)
    buf.clear()
  }

  /**
   * TODO  move to a better home as this is a reusable function.
   * @param fileName
   * @return
   */
  private def getRemoteFile(fileName:String) : File = if (fileName.startsWith("http")){
    logger.info("downloading remote file.. " + fileName)

    //download the file to local repo
    val urlConnection = new java.net.URL(fileName).openConnection()
    val in = urlConnection.getInputStream()
    val file = {
      if (fileName.endsWith(".zip") ){
        val f = new File(Config.tmpWorkDir +  "/delete_row_key_file.zip")
        f.createNewFile()
        val extractedFile:File = f.extractZip
        extractedFile.listFiles().head
      } else if (fileName.endsWith(".gz") ){
        val f = new File(Config.tmpWorkDir +  "/delete_row_key_file.gz")
        logger.info("Creating file: " + f.getAbsolutePath)
        FileUtils.forceMkdir(f.getParentFile())
        f.createNewFile()
        f.extractGzip
      } else {
        val f = new File(Config.tmpWorkDir +  "/delete_row_key_file.csv")
        logger.info("Creating file: " + f.getAbsolutePath)
        FileUtils.forceMkdir(f.getParentFile())
        f.createNewFile()
        f
      }
    }
    val out = new FileOutputStream(file)
    val buffer: Array[Byte] = new Array[Byte](40960)
    var numRead = 0
    var counter = 0
    while ({ numRead = in.read(buffer); numRead != -1 }) {
      counter += numRead
      out.write(buffer, 0, numRead)
      out.flush
    }
    out.flush
    in.close
    out.close
    file
  } else {
    new File(fileName)
  }
}
