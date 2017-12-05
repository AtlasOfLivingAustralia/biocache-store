package au.org.ala.biocache.util

import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool

/**
 * A tool for remapping the unique key for one or more datasets.
 */
object RemapUniqueKey extends Tool {

  def cmd = "remap-unique-id"
  def desc = "Maps one or more fields into a unique key"

  def main(args: Array[String]) {

    var dataResourceUIDs = List[String]()
    var fieldsToUse = List[String]()
    var threads = 4

    val parser = new OptionParser(help) {
      arg("dataResourceUIDs", "the entity (column family in cassandra) to export from", {
        v: String => dataResourceUIDs = v.split(",").map(_.trim).toList
      })
      arg("fieldsToUse", "the file(s) to import, space separated", {
        v: String => fieldsToUse = v.split(",").map(_.trim).toList
      })
      intOpt("t", "threads", "number threads to use", { v: Int => threads = v })
    }

    if (parser.parse(args)) {
      Config.persistenceManager.pageOverLocal("occ", (rowkey, map, tokenRangeId) => {
        val dr = map.getOrElse("dataresourceuid", "")
        if(dataResourceUIDs.contains(dr)){
          val identifyingTerms = fieldsToUse.map { field => map.getOrElse(field.toLowerCase, "")}
          if(!identifyingTerms.filter { value => value != null && value != ""}.isEmpty){
            //generate the key
            val uniqueID = Config.occurrenceDAO.createUniqueID(dr, identifyingTerms, true)
            //insert into table
            Config.persistenceManager.put(uniqueID, "occ_uuid", "value", rowkey, newRecord = true, deleteIfNullValue = false)
          } else {
            println(s"Empty values for identifier for $rowkey")
          }
        }
        true
      }, threads, Array("rowkey", "dataresourceuid") ++ fieldsToUse )
      Config.persistenceManager.shutdown
    }
  }
}
