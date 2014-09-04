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

    val file = new File(Config.tmpWorkDir + "/delrowkeys.out")
    var count = 0
    val start = System.currentTimeMillis
    val out = new BufferedOutputStream(new FileOutputStream(file))
    indexer.writeUUIDsToStream(query, out)
    out.flush
    out.close
    file.foreachLine(line => {
      //use the occ DAO to delete so that the record is added to the dellog cf
      occurrenceDAO.deleteByUuid(line, false, true)
      count = count + 1
    })
    val finished = System.currentTimeMillis

    logger.info("Deleted " + count + " records in " + (finished - start).toFloat / 60000f + " minutes.")
  }

  override def deleteFromIndex = indexer.removeByQuery(query)
}
