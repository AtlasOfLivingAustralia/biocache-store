package au.org.ala.biocache.tool

import java.io.{File, FileOutputStream, InputStream}

import scala.collection.mutable.ArrayBuffer
import au.org.ala.biocache.util.{FileHelper, SFTPTools, StringHelper}
import net.sf.json.util.WebUtils
import java.util.Date

import org.apache.commons.io.FileUtils
import au.org.ala.biocache.Config
import java.util.concurrent.atomic.AtomicLong

/**
 * A record deletor that takes a file of rowkeys to delete.
 */
class FileDelete(fileName: String, hasHeader:Boolean = false) extends RecordDeletor {

  import FileHelper._
  val sftpPattern = """sftp://([a-zA-z\.]*):([0-9a-zA-Z_/\.\-]*)""".r

  //import the file constructs to allow lines to be easily iterated over
  override def deleteFromPersistent {
    logger.info("deleteFromPersistent - Using file name: " + fileName)
    val counter = new AtomicLong(0)
    getRemoteFile(fileName).foreachLine(line => {
      logger.info("Deleting ALA Internal UUID from cassandra : " + line)
      if (line.contains(" ")) {
        throw new RuntimeException("Found a potentially illegal id during delete from file, aborting to avoid corrupting the persistent store: " + line)
      }
      occurrenceDAO.delete(line, removeFromIndex=false, logDeleted=true, removeFromOccUuid=true)
      counter.incrementAndGet()
    })
    logger.info("Records deleted: " + counter.get())
  }

  override def deleteFromIndex {
    logger.info("deleteFromIndex - Using file name: " + fileName)
    var counter = new AtomicLong(0)
    val buf = new ArrayBuffer[String]
    val fieldName = "id"
    getRemoteFile(fileName).foreachLine(line => {
      logger.info("Deleting ALA Internal UUID from Solr : " + line)
      if (line.contains(" ")) {
        throw new RuntimeException("Found a potentially illegal id during delete from file, aborting to avoid corrupting the index: " + line)
      }
      buf += line
      if (buf.size > 999) {
        //val query = "row_key:\"" + buf.mkString("\" OR row_key:\"") + "\""
        val query = fieldName + ":\"" + buf.mkString("\" OR "+ fieldName +":\"") + "\""
        indexer.removeByQuery(query)
        buf.clear()
      }
      counter.incrementAndGet()
    })
    if(!buf.isEmpty) {
      val query = fieldName + ":\"" + buf.mkString("\" OR "+ fieldName +":\"") + "\""
      indexer.removeByQuery(query)
      buf.clear()
    }
    logger.info("Records deleted from index : " + counter.get())
  }

  /**
   * TODO  move to a better home as this is a reusable function.
   * @param fileName
   * @return
   */
  private def getRemoteFile(fileName:String) : File = {
    if (fileName.startsWith("http")){
      logger.info("Downloading delete row key file from http.. " + fileName)
  
      //download the file to local repo
      val urlConnection = new java.net.URL(fileName).openConnection()
      val in = urlConnection.getInputStream()
      try {
        val f = new File(Config.tmpWorkDir +  "/delete_row_key_file.csv")
        logger.info("Creating row key delete file : " + f.getAbsolutePath)
        f.createNewFile()
        downloadFile(f, in)
        f
      } finally {
        in.close()
      }
    } else if (fileName.startsWith("sftp://")){
      logger.info("Downloading delete row key file from sftp.. " + fileName)
      fileName match {
        case sftpPattern(server, filepath) => {
          val f = new File(Config.tmpWorkDir + "/delete_row_key_file.csv")
          logger.info("Creating row key delete file : " + f.getAbsolutePath)
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
      logger.info("Using local row key delete file " + fileName)
      new File(fileName)
    }
  }

  private def downloadFile(file: File, in: InputStream) {
    try {
      val out = new FileOutputStream(file)
      try {
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
      } finally {
        out.close
      }
    } finally {
      in.close
    }
  }
}
