package au.org.ala.biocache.tool

import java.io.{FileOutputStream, BufferedOutputStream, File}
import au.org.ala.biocache.Config

/**
 * Range deletes
 * TODO deletes from index by range not yet implemented
 */
class RangeDeletor(startRowkey: String, endRowkey: String) extends RecordDeletor {

  override def deleteFromPersistent: Unit = {
    var counter = 0
    logger.warn("Range deletor selected : " + startRowkey + " to " + endRowkey)
    Config.persistenceManager.pageOverSelect("occ", (rowKey, map) => {
      if(counter % 10000 == 0){
        println("Deleting row key: " + rowKey + " starts with hash " + rowKey.startsWith("dr342|#"))
      }
      Config.occurrenceDAO.delete(rowKey, false, true)
      counter += 1
      true
    }, startRowkey, endRowkey, 1000, Array[String]():_*)
  }

  override def deleteFromIndex: Unit = logger.warn("Range deletes from index not yet implemented")
}
