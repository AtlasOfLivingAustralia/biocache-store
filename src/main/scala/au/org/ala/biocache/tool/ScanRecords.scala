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

  def main(args:Array[String]){

    var threads:Int = 1
    var local = false
    var csvOutputFile:FileWriter = null
    val parser = new OptionParser(help) {
      intOpt("t", "thread", "The number of threads to use", {v:Int => threads = v } )
      opt("local-only", "Only scan local records",{
        local = true
      })
      opt("of", "output-file", "the file containing coordinates", {
        v: String => csvOutputFile = new FileWriter(v)
      })
    }
    if(parser.parse(args)){
      new ScanRecords().scanRecords(threads, local, csvOutputFile)
      if(csvOutputFile != null){
        csvOutputFile.flush()
        csvOutputFile.close()
      }
    }
  }
}

/**
  * Created by mar759 on 29/07/2016.
  */
class ScanRecords {

  val logger = LoggerFactory.getLogger("ScanLocalRecords")

  def scanRecords(threads:Int, local:Boolean, csvOutputFile:FileWriter) : Unit = {

    val start = System.currentTimeMillis()
    val synchronizedMap = new scala.collection.mutable.LinkedHashMap[String, Int]()
      with scala.collection.mutable.SynchronizedMap[String, Int]

    val totalScanned = if(local) {
      Config.persistenceManager.pageOverLocal("occ", (key, map) => {
        synchronized {
          val dr = map.getOrElse("dataresourceuid", "")
          if (dr != "") {
            val count = synchronizedMap.getOrElse(dr, 0)
            synchronizedMap.put(dr, count + 1)
          }
        }
        true
      }, threads, Array[String]("rowkey", "dataresourceuid"))
    } else {
      Config.persistenceManager.pageOverSelect("occ", (key, map) => {
        synchronized {
          val dr = map.getOrElse("dataresourceuid", "")
          if (dr != "") {
            val count = synchronizedMap.getOrElse(dr, 0)
            synchronizedMap.put(dr, count + 1)
          }
        }
        true
      }, "", "", 1000, "rowkey", "dataresourceuid")
    }

    val end = System.currentTimeMillis()
    val timeInMinutes = ((end-start).toFloat / 1000f / 60f)
    val timeInSecs = ((end-start).toFloat / 1000f  )
    val numberOfResources = synchronizedMap.size
    logger.info(s"Total records scanned : $totalScanned from $numberOfResources resources in $timeInSecs seconds (or $timeInMinutes minutes)")

    synchronizedMap.foreach { case (dataResource:String, count:Int) =>
      logger.info(dataResource + " : " + count)
      if(csvOutputFile != null){
        csvOutputFile.write(dataResource + " , " + count + "\n")
      }
    }

    logger.info(s"Scan complete")
  }
}
