package au.org.ala.biocache.tool

import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.util.OptionParser
import org.slf4j.LoggerFactory

object DeleteLocalDataResource extends Tool {

  def cmd = "delete-resource-local-node"

  def desc = "Delete records on a local database node"

  def main(args: Array[String]) {

    var threads: Int = 1
    var drs: Seq[String] = List()
    var startTokenRangeIdx = 0
    var entityName = "occ"

    val parser = new OptionParser(help) {

      intOpt("t", "thread", "The number of threads to use", { v: Int => threads = v })
      opt("entityName", "entityName", "entityName, defaults to occ", {
        v: String => entityName = v
      })
      opt("dr", "data-resource-list", "comma separated list of drs to process", {
        v: String =>
          drs = v.trim.split(",").map {
            _.trim
          }
      })
      intOpt("stk", "start-token-range-idx", "the idx of the token range to start at. Typically a value between 0 and 1024." +
        "This is useful when a long running process fails for some reason.", {
        v: Int => startTokenRangeIdx = v
      })
    }

    if (parser.parse(args)) {
      new DeleteLocalDataResource().delete(entityName, threads, drs, startTokenRangeIdx)
    }
  }
}

class DeleteLocalDataResource {

  val logger = LoggerFactory.getLogger("DeleteLocalDataResource")

  def delete(entityName: String, threads: Int, drs: Seq[String], startTokenRangeIdx: Int): Unit = {

    var deleted = 0

    val recordsRead = Config.persistenceManager.pageOverLocal("occ", (rowkey, map, batchID) => {
      val drUid = map.getOrElse("dataResourceUid", "")
      val rowkey = map.getOrElse("rowkey", "")
      if (drUid != "") {
        if (drs.contains(drUid)) {
          Config.persistenceManager.delete(rowkey, entityName)
          synchronized {
            deleted += 1
          }
        }
      }
      true
    }, threads, Array("rowkey", "dataResourceUid"))
    logger.info(s"Records read $recordsRead. Records deleted: $deleted")
  }
}

