package au.org.ala.biocache.tool

import java.io.File
import scala.collection.mutable.ArrayBuffer
import au.org.ala.biocache.util.FileHelper

/**
 * A record deletor that takes a file of rowkeys to delete.
 */
class FileDelete(fileName: String) extends RecordDeletor {

  import FileHelper._

  //import the file constructs to allow lines to be easily iterated over
  override def deleteFromPersistent {
    new File(fileName).foreachLine(line =>
      occurrenceDAO.delete(line, false, true)
    )
  }

  override def deleteFromIndex {
    val buf = new ArrayBuffer[String]

    new File(fileName).foreachLine(line => {
      buf += line
      if (buf.size > 999) {
        val query = "row_key:\"" + buf.mkString("\" OR row_key:\"") + "\""
        indexer.removeByQuery(query)
        buf.clear()
      }
    })
  }
}
