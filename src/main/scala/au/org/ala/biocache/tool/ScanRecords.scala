package au.org.ala.biocache.tool

import java.io.FileWriter

import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.util.OptionParser
import org.slf4j.LoggerFactory

/**
  * A utility to scan records on a node and retrieve counts by data resource.
  */
object ScanRecords extends Tool {

  def cmd = "scan-node"

  def desc = "Scan all records on a node"

  def main(args: Array[String]) {

    var threads: Int = 1
    var local = false
    var csvOutputFile: FileWriter = null
    var aggregateField = "dataResourceUid"

    val parser = new OptionParser(help) {
      intOpt("t", "thread", "The number of threads to use", { v: Int => threads = v })
      opt("local-only", "Only scan local records", {
        local = true
      })
      opt("of", "output-file", "output counts in CSV to the supplied file", {
        v: String => csvOutputFile = new FileWriter(v)
      })
      opt("af", "aggregate-field", "output aggregate counts for the supplied database field. Default is " + aggregateField, {
        v: String => aggregateField = v
      })
    }

    if (parser.parse(args)) {
      new ScanRecords().scanRecords(threads, local, aggregateField, csvOutputFile)
      if (csvOutputFile != null) {
        csvOutputFile.flush()
        csvOutputFile.close()
      }
    }
  }
}

/**
  * Class for running a table scan.
  */
class ScanRecords {

  val logger = LoggerFactory.getLogger("ScanLocalRecords")

  def scanRecords(threads: Int, local: Boolean, aggregateField:String, csvOutputFile: FileWriter): Unit = {

    val start = System.currentTimeMillis()
    val synchronizedMap = new scala.collection.mutable.LinkedHashMap[String, Int]()
      with scala.collection.mutable.SynchronizedMap[String, Int]

    var counter = 0
    if (local) {
      Config.persistenceManager.pageOverLocal("occ", (key, map, tokenRangeIdx) => {
        synchronized {
          counter += 1
          val dr = map.getOrElse(aggregateField, "")
          if (dr != "") {
            val count = synchronizedMap.getOrElse(dr, 0)
            synchronizedMap.put(dr, count + 1)
          }
          if(counter % 10000 == 0){
            logger.info(s"Total records scanned : $counter")
          }
        }
        true
      }, threads, Array[String]("rowkey", aggregateField))
    } else {
      Config.persistenceManager.pageOverSelect("occ", (key, map) => {
        synchronized {
          counter += 1
          val dr = map.getOrElse(aggregateField, "")
          if (dr != "") {
            val count = synchronizedMap.getOrElse(dr, 0)
            synchronizedMap.put(dr, count + 1)
          }
          if(counter % 10000 == 0){
            logger.info(s"Total records scanned : $counter")
          }
        }
        true
      }, 1000, threads, "rowkey", aggregateField)
    }

    val end = System.currentTimeMillis()
    val timeInMinutes = ((end - start).toFloat / 1000f / 60f)
    val timeInSecs = ((end - start).toFloat / 1000f)
    val numberOfResources = synchronizedMap.size
    logger.info(s"Total records scanned : $counter from $numberOfResources resources in $timeInSecs seconds (or $timeInMinutes minutes)")

    synchronizedMap.foreach { case (dataResource: String, count: Int) =>
      logger.info(dataResource + " : " + count)
      if (csvOutputFile != null) {
        csvOutputFile.write(dataResource + "," + count + "\n")
      }
    }

    logger.info(s"Scan complete. Records scanned: " + counter)
  }
}
