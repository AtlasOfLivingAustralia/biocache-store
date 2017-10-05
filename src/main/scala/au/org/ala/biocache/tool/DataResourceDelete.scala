package au.org.ala.biocache.tool

import java.io.File

import au.org.ala.biocache.Store
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.util.{FileHelper, OptionParser}

object DataResourceDelete extends Tool {

  def cmd = "delete-resource"

  def desc = "Delete one or more data resources"

  def main(args: Array[String]) {
    var resources = Array[String]()
    val parser = new OptionParser(help) {
      arg("data-resource-uid", "Comma separated list of UID data resource to delete, e.g. dr1", {
        v: String => resources = v.split(",").map(v => v.trim)
      })
    }

    if (parser.parse(args)) {
      resources.foreach(drUid => {
        val drvd = new DataResourceDelete(drUid)
        println("Delete from storage: " + drUid)
        drvd.deleteFromPersistent
        println("Delete from index: " + drUid)
        drvd.deleteFromIndex
        println("Finished delete for : " + drUid)
      })
    }
  }
}

/**
  * A utility to delete a data resource
  */
class DataResourceDelete(dataResource: String) extends RecordDeletor {

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
        occurrenceDAO.delete(line, false, true)

        if (count % 1000 == 0) {
          logger.info("records deleted: " + count)
        }
      })
    } else {
      println("Using query for dataResourceUid")
      pm.pageOverSelect("occ", (guid, map) => {
        occurrenceDAO.delete(guid, false, true)

        true
      }, "dataResourceUid", dataResource, 1000, "rowkey")
    }

    val finished = System.currentTimeMillis

    println("Deleted " + count + " records in " + (finished - start).toFloat / 60000f + " minutes.")
  }

  override def deleteFromIndex {
    indexer.removeByQuery("data_resource_uid:" + dataResource)
  }
}
