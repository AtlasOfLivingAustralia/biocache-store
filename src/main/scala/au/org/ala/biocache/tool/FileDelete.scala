package au.org.ala.biocache.tool

import java.io.{File, FileOutputStream, InputStream}

import scala.collection.mutable.ArrayBuffer
import au.org.ala.biocache.util.{FileHelper, SFTPTools, StringHelper}
import net.sf.json.util.WebUtils
import java.util.Date

import org.apache.commons.io.FileUtils
import au.org.ala.biocache.Config

/**
 * A record deletor that takes a file of rowkeys to delete.
 */
class FileDelete(fileName: String, hasHeader:Boolean = false) extends RecordDeletor {

  import FileHelper._
  val sftpPattern = """sftp://([a-zA-z\.]*):([0-9a-zA-Z_/\.\-]*)""".r

  //import the file constructs to allow lines to be easily iterated over
  override def deleteFromPersistent {
    logger.info("Using file name: " + fileName)
    var counter = 0
    getRemoteFile(fileName).foreachLine(line => {
      logger.info("Deleting ID : " + line)
      if (line.contains(" ")) {
        throw new RuntimeException("Found a potentially illegal id during delete from file, aborting to avoid corrupting the persistent store: " + line)
      }
      occurrenceDAO.delete(line, false, true)
      counter += 1
    })
    logger.info("Records deleted: " + counter)
  }

  override def deleteFromIndex {
    logger.info("deleteFromIndex - Using file name: " + fileName)
    var counter = 0
    val buf = new ArrayBuffer[String]
    val fieldName = "id"
    getRemoteFile(fileName).foreachLine(line => {
      buf += line
      counter += 1
      if (line.contains(" ")) {
        throw new RuntimeException("Found a potentially illegal id during delete from file, aborting to avoid corrupting the index: " + line)
      }
      if (buf.size > 999) {
        //val query = "row_key:\"" + buf.mkString("\" OR row_key:\"") + "\""
        val query = fieldName + ":\"" + buf.mkString("\" OR "+ fieldName +":\"") + "\""
        indexer.removeByQuery(query)
        buf.clear()
      }
    })
    val query = fieldName + ":\"" + buf.mkString("\" OR "+ fieldName +":\"") + "\""
    indexer.removeByQuery(query)
    logger.info("Records deleted from index : " + counter)
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
    val f = new File(Config.tmpWorkDir +  "/delete_row_key_file.csv")
    logger.info("Creating file: " + f.getAbsolutePath)
    f.createNewFile()
    downloadFile(f, in)
    f
  } else  if(fileName.startsWith("sftp://")){
    fileName match {
      case sftpPattern(server, filepath) => {
        val f = new File(Config.tmpWorkDir + "/delete_row_key_file.csv")
        SFTPTools.scpFile(
          server,
          Config.getProperty("uploadUser"),
          Config.getProperty("uploadPassword"),
          filepath,
          f)
        f
      }
    }
  } else {
    new File(fileName)
  }

  private def downloadFile(file: File, in: InputStream) {
    val out = new FileOutputStream(file)
    val buffer: Array[Byte] = new Array[Byte](40960)
    var numRead = 0
    var counter = 0
    while ( {
      numRead = in.read(buffer)
      numRead != -1
    }) {
      counter += numRead
      out.write(buffer, 0, numRead)
      out.flush
    }
    out.flush
    in.close
    out.close
  }
}
