package au.org.ala.biocache.util

import java.io.{FileOutputStream, BufferedOutputStream, File}
import au.org.ala.biocache.Config

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
    indexer.writeRowKeysToStream(query, out)
    out.flush
    out.close
    file.foreachLine(line => {
      //pm.delete(line, "occ")
      //use the occ DAO to delete so that the record is added to the dellog cf
      occurrenceDAO.delete(line, false, true)
      count = count + 1
    })
    val finished = System.currentTimeMillis

    logger.info("Deleted " + count + " records in " + (finished - start).toFloat / 60000f + " minutes.")
  }

  override def deleteFromIndex = indexer.removeByQuery(query)
}
