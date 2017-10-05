package au.org.ala.biocache.tool

import java.io.File

import au.org.ala.biocache.Store
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.util.FileHelper

/**
  * A deletor that marks occurrence records, for a specific data resource, as deleted before
  * removing them at a later time.
  */
class DataResourceVirtualDelete(dataResource: String) extends RecordDeletor {

  import FileHelper._

  override def deleteFromPersistent {
    //page over all the records for the data resource deleting them
    var count = 0
    val start = System.currentTimeMillis

    val keyFile: File = Store.rowKeyFile(dataResource)
    if (keyFile.exists()) {
      println("Using rowKeyFile " + keyFile.getPath)

      keyFile.foreachLine(line => {
        count += 1
        occurrenceDAO.setDeleted(line, true)

        if (count % 1000 == 0) {
          logger.info("records deleted: " + count)
        }
      })
    } else {
      println("Using query for dataResourceUid")
      pm.pageOverSelect("occ", (guid, map) => {
        count += 1
        occurrenceDAO.setDeleted(guid, true)

        if (count % 1000 == 0) {
          logger.info("records deleted: " + count)
        }
        true
      }, "dataResourceUid", dataResource, 1000, "rowkey")
    }

    val finished = System.currentTimeMillis

    println("Marked " + count + " records as deleted in " + (finished - start).toFloat / 60000f + " minutes.")
  }

  override def deleteFromIndex = indexer.removeByQuery("data_resource_uid:" + dataResource)

  /**
    * Physically deletes all records where deleted=true in the persistence manager.
    */
  def physicallyDeleteMarkedRecords {

    //page over all the records for the data resource deleting them
    var count = 0
    val start = System.currentTimeMillis

    val keyFile: File = Store.rowKeyFile(dataResource)
    if (keyFile.exists()) {
      println("Using rowKeyFile " + keyFile.getPath)

      keyFile.foreachLine(line => {
        val delete = pm.get(line, "occ", FullRecordMapper.deletedColumn)

        if ("true".equals(delete.get)) {
          //use the occ DAO to delete so that the record is added to the dellog cf
          occurrenceDAO.delete(line, false, true)
          count = count + 1
        }

        if (count % 1000 == 0) {
          logger.info("records deleted: " + count)
        }
      })
    } else {
      println("Using query for dataResourceUid")
      pm.pageOverSelect("occ", (guid, map) => {
        val delete = map.getOrElse(FullRecordMapper.deletedColumn, "false")
        if ("true".equals(delete)) {
          //use the occ DAO to delete so that the record is added to the dellog cf
          occurrenceDAO.delete(guid, false, true)
          count = count + 1
        }

        true
      }, "dataResourceUid", dataResource, 1000, "rowkey", FullRecordMapper.deletedColumn)
    }
  }
}
