package au.org.ala.biocache.tool

import au.org.ala.biocache.Config
import au.org.ala.biocache.persistence.Cassandra3PersistenceManager
import au.org.ala.biocache.tool.SyncLocTable.logger
import org.slf4j.LoggerFactory

/**
  * Utility to sync loc table definition with fields available for sampling.
  */
object SyncLocTable extends au.org.ala.biocache.cmd.Tool {

  def cmd = "sync-loc-table"

  def desc = "Synchronise the loc table with the list of available layers. This needs to be ran prior to sampling."

  protected val logger = LoggerFactory.getLogger("SyncLocTable")

  def main(args: Array[String]) {
    new SyncLocTable().sync
    Config.persistenceManager.shutdown
  }
}

class SyncLocTable {

  def sync  {
    val fields = Config.fieldsToSample(true)

    val pm = Config.persistenceManager.asInstanceOf[Cassandra3PersistenceManager]

    val fieldsInTable = pm.listFieldsForEntity("loc")

    //need to get table definition of loc and do alter table statements where applicable
    fields.foreach { field =>
      if(fieldsInTable.contains(field)){
        logger.info(s"$field is present")
      } else {
        logger.info(s"$field is NOT present")
        try {
          pm.addFieldToEntity("loc", field)
        } catch {
          case e: Exception => {
            // sync may run in parallel. Do not report exceptions when the new field already exists.
            if (!pm.listFieldsForEntity("loc").contains(field)) {
              logger.error(s"Problem adding field $field to loc table...", e)
            }
          }
        }
      }
    }
    logger.info("Sync complete")
  }
}
