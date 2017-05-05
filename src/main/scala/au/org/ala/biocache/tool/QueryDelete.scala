package au.org.ala.biocache.tool

import java.io.{FileOutputStream, BufferedOutputStream, File}
import au.org.ala.biocache.Config
import au.org.ala.biocache.util.FileHelper

/**
 * Utility to delete records matching a query.
 */
class QueryDelete(query: String) extends RecordDeletor {

  import FileHelper._

  override def deleteFromPersistent() = {

    val delFile = Config.tmpWorkDir + "/delrowkeys.out"
    val file = new File(delFile)
    var count = 0
    val start = System.currentTimeMillis
    val out = new BufferedOutputStream(new FileOutputStream(file))
    logger.info("Retrieving a list of UUIDs from index....")
    indexer.writeUUIDsToStream(query, out)
    logger.info(s"Retrieved a list of UUIDs from index. IDs written to $delFile")
    out.flush
    out.close
    file.foreachLine { line =>
      //use the occ DAO to delete so that the record is added to the dellog cf
      occurrenceDAO.deleteByUuid(line, false, true)
      count = count + 1
    }
    val finished = System.currentTimeMillis

    logger.info("Deleted " + count + " records in " + (finished - start).toFloat / 60000f + " minutes.")
  }

  override def deleteFromIndex = indexer.removeByQuery(query)
}
