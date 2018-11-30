package au.org.ala.biocache.tool

import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.util.OptionParser
import org.slf4j.LoggerFactory

/**
 * Utility to delete the contents of a column.
 * In a cluster, this should be ran on each node.
 */
object DeleteLocalColumn extends Tool {

  def cmd = "delete-column-local-node"

  def desc = "Delete value in columns (if populated) on a local database node. This does a full table scan and should be ran" +
    " on each node in the cluster."

  def main(args: Array[String]) {

    var threads: Int = 1
    var columns: Seq[String] = List()
    var startTokenRangeIdx = 0
    var entityName = "occ"

    val parser = new OptionParser(help) {

      intOpt("t", "thread", "The number of threads to use", { v: Int => threads = v })
      opt("entityName", "entityName", "entityName, defaults to occ", {
        v: String => entityName = v
      })
      opt("cl", "column-list", "comma separated list of columns to clear", {
        v: String =>
          columns = v.trim.split(",").map {
            _.trim
          }
      })
      intOpt("stk", "start-token-range-idx", "the idx of the token range to start at. Typically a value between 0 and 1024." +
        "This is useful when a long running process fails for some reason.", {
        v: Int => startTokenRangeIdx = v
      })
    }

    if (parser.parse(args)) {
      new DeleteLocalColumn().delete(entityName, threads, columns, startTokenRangeIdx)
    }
  }
}

class DeleteLocalColumn {

  val logger = LoggerFactory.getLogger("DeleteLocalDataResource")

  def delete(entityName: String, threads: Int, columns: Seq[String], startTokenRangeIdx: Int): Unit = {
    var deleted = 0
    val recordsRead = Config.persistenceManager.pageOverLocal("occ", (rowkey, map, batchID) => {
      columns.foreach { column =>
        val columnValue = map.getOrElse(column, "")
        if (columnValue != ""){
          Config.persistenceManager.put(rowkey, entityName, column, null, false, true)
          deleted += 1
        }
      }
      true
    }, threads, Array("rowkey") ++ columns)
    logger.info(s"Values read $recordsRead. Values deleted: $deleted")
  }
}

