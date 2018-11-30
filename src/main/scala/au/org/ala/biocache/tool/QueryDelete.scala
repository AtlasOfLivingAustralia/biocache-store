package au.org.ala.biocache.tool

import java.io.{FileOutputStream, BufferedOutputStream, File}
import au.org.ala.biocache.Config
import au.org.ala.biocache.util.FileHelper
import java.util.concurrent.atomic.AtomicInteger

/**
 * Utility to delete records matching a query.
 */
class QueryDelete(query: String) extends RecordDeletor {

  import FileHelper._

  override def deleteFromPersistent() = {

    val delFile = Config.tmpWorkDir + "/delrowkeys.out"
    val file = new File(delFile)
    val count = new AtomicInteger(0)
    val start = System.currentTimeMillis
    val out = new BufferedOutputStream(new FileOutputStream(file))
    try {
      logger.info("Retrieving a list of UUIDs from index....")
      indexer.writeUUIDsToStream(query, out)
      logger.info(s"Retrieved a list of UUIDs from index. IDs written to $delFile")
      out.flush
    } finally {
      out.close
    }
    file.foreachLine { line =>
      occurrenceDAO.delete(line, removeFromIndex=false, logDeleted=true)
      count.incrementAndGet()
    }
    val finished = System.currentTimeMillis

    logger.info("Deleted " + count.get() + " records in " + (finished - start).toFloat / 60000f + " minutes.")
  }

  override def deleteFromIndex = {  indexer.removeByQuery(query)  }
}
