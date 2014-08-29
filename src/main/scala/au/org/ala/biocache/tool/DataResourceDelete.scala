package au.org.ala.biocache.tool

import au.org.ala.biocache.util.OptionParser
import au.org.ala.biocache.cmd.Tool

object DataResourceDelete extends Tool {

  def cmd = "delete-resource"
  def desc = "Delete one or more data resources"

  def main(args:Array[String]){
    var resources = Array[String]()
    val parser = new OptionParser(help) {
      arg("data-resource-uid", "Comma separated list of UID data resource to delete, e.g. dr1", {
        v:String => resources = v.split(",").map(v => v.trim)
      })
    }

    if(parser.parse(args)) {
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
class DataResourceDelete(dataResource:String) extends RecordDeletor {

    override def deleteFromPersistent {
        //page over all the records for the data resource deleting them
        var count = 0
        val start = System.currentTimeMillis
        val startUuid = dataResource +"|"
        val endUuid = startUuid + "~"

        pm.pageOverSelect("occ", (guid,map)=>{
            //use the occ DAO to delete so that the record is added to the dellog cf
            occurrenceDAO.delete(guid, false, true)
            count= count +1
            true
        }, startUuid, endUuid, 1000, "rowKey", "uuid")
        val finished = System.currentTimeMillis

      println("Deleted " + count + " records in "  + (finished -start).toFloat / 60000f + " minutes.")
    }
    override def deleteFromIndex {
        indexer.removeByQuery("data_resource_uid:" +dataResource)
    }
}
