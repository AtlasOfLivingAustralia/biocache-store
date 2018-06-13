package au.org.ala.biocache.tool

import java.io.FileWriter

import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.util.OptionParser
import org.apache.commons.lang3.StringUtils
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
    var csvFieldOutputFile: FileWriter = null
    var aggregateField = "dataResourceUid"

    val parser = new OptionParser(help) {
      intOpt("t", "thread", "The number of threads to use", { v: Int => threads = v })
      opt("local-only", "Only scan local records", {
        local = true
      })
      opt("of", "output-file", "output counts in CSV to the supplied file", {
        v: String => csvOutputFile = new FileWriter(v)
      })
      opt("fof", "fields-output-file", "field counts in CSV to the supplied file", {
        v: String => csvFieldOutputFile = new FileWriter(v)
      })
      opt("af", "aggregate-field", "output aggregate counts for the supplied database field. Default is " + aggregateField, {
        v: String => aggregateField = v
      })
    }

    if (parser.parse(args)) {

      if(csvFieldOutputFile != null){
        new ScanRecords().fieldScanRecords(threads, local, aggregateField, csvOutputFile, csvFieldOutputFile)
        if (csvOutputFile != null) {
          csvOutputFile.flush()
          csvOutputFile.close()
        }
        if (csvFieldOutputFile != null) {
          csvFieldOutputFile.flush()
          csvFieldOutputFile.close()
        }

      } else {
        new ScanRecords().scanRecords(threads, local, aggregateField, csvOutputFile)
        if (csvOutputFile != null) {
          csvOutputFile.flush()
          csvOutputFile.close()
        }
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
    var deletedRecords = 0
    var noResourceRecords = 0

    if (local) {
      Config.persistenceManager.pageOverLocal("occ", (key, map, tokenRangeIdx) => {
        synchronized {
          counter += 1
          val dateDeleted = map.getOrElse("dateDeleted", "")
          if(dateDeleted == "") {
            val dr = map.getOrElse(aggregateField, "")
            if (dr != "") {
              val count = synchronizedMap.getOrElse(dr, 0)
              synchronizedMap.put(dr, count + 1)
            } else {
              noResourceRecords += 1
            }
          } else {
            deletedRecords += 1
          }
          if(counter % 10000 == 0){
            logger.info(s"Total records scanned : $counter")
          }
        }
        true
      }, threads, Array[String]("rowkey", "dateDeleted", aggregateField))
    } else {
      Config.persistenceManager.pageOverSelect("occ", (key, map) => {
        synchronized {
          counter += 1
          val dateDeleted = map.getOrElse("dateDeleted", "")
          if(dateDeleted == "") {
            val dr = map.getOrElse(aggregateField, "")
            if (dr != "") {
              val count = synchronizedMap.getOrElse(dr, 0)
              synchronizedMap.put(dr, count + 1)
            } else {
              noResourceRecords += 1
            }
          } else {
            deletedRecords += 1
          }
          if(counter % 10000 == 0){
            logger.info(s"Total records scanned : $counter")
          }
        }
        true
      }, 1000, threads, "rowkey", "dateDeleted", aggregateField)
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

    logger.info(s"Scan complete. Records scanned: " + counter + ". Records marked as deleted: " + deletedRecords + ", Number of associated data resource: " + noResourceRecords)
  }

  def fieldScanRecords(threads: Int, local: Boolean, aggregateField:String, csvOutputFile: FileWriter, fieldsOutputFile: FileWriter): Unit = {

    val start = System.currentTimeMillis()
    val dataResourceMap = new scala.collection.mutable.LinkedHashMap[String, Int]()
      with scala.collection.mutable.SynchronizedMap[String, Int]
    val fieldsMap = new scala.collection.mutable.LinkedHashMap[String, Int]()
      with scala.collection.mutable.SynchronizedMap[String, Int]

    var counter = 0
    var deletedRecords = 0
    var noResourceRecords = 0

    if (local) {
      Config.persistenceManager.pageOverLocal("occ", (key, map, tokenRangeIdx) => {
        synchronized {
          counter += 1
          val dateDeleted = map.getOrElse("dateDeleted", "")
          if(dateDeleted == "") {
            val dr = map.getOrElse(aggregateField, "")
            if (dr != "") {
              val count = dataResourceMap.getOrElse(dr, 0)
              dataResourceMap.put(dr, count + 1)
            } else {
              noResourceRecords += 1
            }
            map.keySet.foreach { key =>
              val value = map.getOrElse(key, "")
              if(StringUtils.isNotEmpty(value)){
                val count = fieldsMap.getOrElse(key, 0)
                fieldsMap.put(key, count + 1)
              }
            }
          } else {
            deletedRecords += 1
          }
          if(counter % 10000 == 0){
            logger.info(s"Total records scanned : $counter")
          }
        }
        true
      }, threads, Array[String]())
    } else {
      Config.persistenceManager.pageOverSelect("occ", (key, map) => {
        synchronized {
          counter += 1
          val dateDeleted = map.getOrElse("dateDeleted", "")
          if(dateDeleted == "") {
            val dr = map.getOrElse(aggregateField, "")
            if (dr != "") {
              val count = dataResourceMap.getOrElse(dr, 0)
              dataResourceMap.put(dr, count + 1)
            } else {
              noResourceRecords += 1
            }
            map.keySet.foreach { key =>
              val value = map.getOrElse(key, "")
              if(StringUtils.isNotEmpty(value)){
                val count = fieldsMap.getOrElse(key, 0)
                fieldsMap.put(key, count + 1)
              }
            }
          } else {
            deletedRecords += 1
          }
          if(counter % 10000 == 0){
            logger.info(s"Total records scanned : $counter")
          }
        }
        true
      }, 1000, threads)
    }

    val end = System.currentTimeMillis()
    val timeInMinutes = ((end - start).toFloat / 1000f / 60f)
    val timeInSecs = ((end - start).toFloat / 1000f)
    val numberOfResources = dataResourceMap.size
    logger.info(s"Total records scanned : $counter from $numberOfResources resources in $timeInSecs seconds (or $timeInMinutes minutes)")

    dataResourceMap.foreach { case (dataResource: String, count: Int) =>
      logger.info(dataResource + " : " + count)
      if (csvOutputFile != null) {
        csvOutputFile.write(dataResource + "," + count + "\n")
      }
    }

    fieldsMap.foreach { case (field: String, count: Int) =>
      logger.info(field + " : " + count)
      if (fieldsOutputFile != null) {
        fieldsOutputFile.write(field + "," + count + "\n")
      }
    }

    logger.info(s"Scan complete. Records scanned: " + counter + ". Records marked as deleted: " + deletedRecords + ", Number of associated data resource: " + noResourceRecords)
  }

}
