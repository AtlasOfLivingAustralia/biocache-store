package au.org.ala.biocache.tool

import au.org.ala.biocache.Config
import scala.collection.mutable.ArrayBuffer
import au.org.ala.biocache.parser.DateParser
import au.org.ala.biocache.model.FullRecord
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.util.{OptionParser, FileHelper}
import au.org.ala.biocache.cmd.{IncrementalTool, Tool}
import org.slf4j.LoggerFactory

/**
 * Provides the cleanup mechanisms for a data resource.  Includes:
 *
 * Marking records as deleted and removing not updated raw fields
 * Removing "Deleted records" 
 */
object ResourceCleanupTask extends Tool with IncrementalTool {

  import FileHelper._

  val logger = LoggerFactory.getLogger("ResourceCleanupTask")

  def cmd = "resource-cleanup"

  def desc = "Resource cleanup tool for removing records or columns which have not been updated since a supplied date."

  def main(args: Array[String]) {

    var dataResourceUid = ""
    //default to the current date
    var lastLoadDate = org.apache.commons.lang.time.DateFormatUtils.format(new java.util.Date, "yyyy-MM-dd") + "T00:00:00Z"
    var defaultDate = lastLoadDate
    var removeRows = false
    var removeColumns = false
    var removeDeleted = false
    var test = false
    var start: Option[String] = None
    var end: Option[String] = None
    var columns = Array[String]() //stores the columns to keep or remove depending on the args
    var isInclusiveList = true // the record need to include all the columns
    var filename: Option[String] = None
    var checkRowKeyFile = false

    val parser = new OptionParser(help) {
      arg("data-resource-uid", "The data resource on which to perform the clean up.", {
        v: String => dataResourceUid = v
      })
      arg("type", "The type of cleanup to perform." +
        "Either all, columns, rows, delete. " +
        "\n\t\tcolumns - removes the columns that were not modified since the " +
        "last date \n\t\trows - marks records as deleted when they have not been reloaded " +
        "since the supplied date \n\t\tdelete - removes the deleted record from occ and places them into the dellog ", {
        v: String => v match {
          case "all" => removeRows = true; removeColumns = true; removeDeleted = true
          case "columns" => removeColumns = true;
          case "rows" => removeRows = true
          case "delete" => removeDeleted = true
          case _ =>
        }
      })
      opt("f", "file", "<absolute path to file>", "The name of the file to incrementally remove obsolete columns from", {
        value: String => filename = Some(value)
      })
      opt("d", "date", "<date last loaded yyyy-MM-dd format>", "The date of the last load.  Any records that have not been updated since this date will be marked as deleted.", {
        value: String => lastLoadDate = value + "T00:00:00Z"
      })
      opt("s", "start", "<starting rowkey>", "The row key to start looking for deleted records", {
        value: String => start = Some(value)
      })
      opt("e", "end", "<ending rowkey>", "The row key to stop looking for deleted records", {
        value: String => end = Some(value)
      })
      opt("test", "Simulates the cleanup without removing anything.  Useful to run to determine whether or not you are happy with the load.", {
        test = true
      })
      opt("delcols", "Delete the columns in the supplied list", {
        isInclusiveList = false
      })
      opt("c", "cols", "<list of columns>", "Comma separated list of columns to perform the operation on", {
        value: String => columns = value.split(",")
      })
      opt("crk", "check for row key file", {
        checkRowKeyFile = true
      })
    }

    if (parser.parse(args)) {

      if (checkRowKeyFile && dataResourceUid != "") {
        val (hasRowKeyFile, filePath) = hasRowKey(dataResourceUid)
        filename = filePath
      }

      val checkDate = DateParser.parseStringToDate(lastLoadDate)
      logger.info(s"Attempting to cleanup $dataResourceUid based on a last load date of $checkDate rows: $removeRows  columns: $removeColumns, start:  $start,end:  $end.")
      if (checkDate.isDefined && columns.length == 0) {
        if (removeRows) {
          modifyRecord(dataResourceUid, checkDate.get, start, end, test)
        }

        if (removeColumns) {
          if (filename.isDefined) {
            removeObsoleteColumnsIncremental(new java.io.File(filename.get), checkDate.get.getTime(), test)
          } else {
            removeObsoleteColumns(dataResourceUid, checkDate.get.getTime(), start, end, test)
          }
        }

        if (removeDeleted) {
          removeDeletedRecords(dataResourceUid, checkDate.get, start, end, test)
        }

      } else if (columns.length > 0) {
        //running this with a date will be slower
        if (!checkDate.isDefined || checkDate.get.equals(DateParser.parseStringToDate(defaultDate).get)) {
          if (isInclusiveList) {
            removeRawRecordColumnsNotInList(dataResourceUid, columns, start, end, test)
          } else {
            removeSpecifiedColumns(dataResourceUid, columns, start, end, test)
          }
        } else {
          if (isInclusiveList) {
            removeRawRecordColumnsNotInListByDate(dataResourceUid, columns, checkDate.get.getTime(), start, end, test)
          } else {
            removeSpecifiedColumnsByDate(dataResourceUid, columns, checkDate.get.getTime(), start, end, test)
          }
        }
      }
    }
  }

  def removeRawRecordColumnsNotInList(dr: String, colsToKeep: Array[String], start: Option[String], end: Option[String], test: Boolean = false) {
    logger.info("Starting to remove raw record columns not in " + colsToKeep.toList)
    val startUuid = start.getOrElse(dr + "|")
    val endUuid = end.getOrElse(startUuid + "~")
    val pm = Config.persistenceManager
    val valueSet = new scala.collection.mutable.HashSet[String]
    var totalRecords = 0
    var totalRecordModified = 0
    var totalColumnsRemoved = 0
    val fullRecord = new FullRecord
    val valuesToIgnore = Array("uuid", "originalSensitiveValues", "rowKey")
    pm.pageOverAll("occ", (guid, map) => {
      totalRecords += 1
      val colToDelete = new ArrayBuffer[String]
      map.keySet.foreach(fieldName => {
        fieldName match {
          case it if fullRecord.hasNestedProperty(fieldName) => {
            //check to see if the column is in the columns to keep list
            if (!colsToKeep.contains(fieldName) && !valuesToIgnore.contains(fieldName)) {
              colToDelete += fieldName
              valueSet += fieldName
              totalColumnsRemoved += 1
            }
          }
          case _ => //ignore
        }
      })
      if (colToDelete.size > 0) {
        totalRecordModified += 1
        if (!test) {
          //delete all the columns that were not updated
          Config.persistenceManager.deleteColumns(guid, "occ", colToDelete.toArray: _*)
        }
      }
      true
    }, startUuid, endUuid, 1000)
    logger.info("Finished cleanup for columns")
    logger.info("List of columns that have been removed from one or more records:")
    logger.info(valueSet.toList.toString())
    logger.info("total records changed: " + totalRecordModified + " out of " + totalRecords + ". " + totalColumnsRemoved + " columns were removed from cassandra")
  }

  def removeSpecifiedColumns(dr: String, colsToDelete: Array[String], start: Option[String], end: Option[String], test: Boolean = false) {
    logger.info("Starting to remove all columns in " + colsToDelete.toList)
    val startUuid = start.getOrElse(dr + "|")
    val endUuid = end.getOrElse(startUuid + "~")
    val pm = Config.persistenceManager
    var totalRecords = 0
    var totalRecordModified = 0
    var totalColumnsRemoved = 0
    val fullRecord = new FullRecord
    val valueSet = new scala.collection.mutable.HashSet[String]
    pm.pageOverAll("occ", (guid, map) => {
      totalRecords += 1
      val colToDelete = new ArrayBuffer[String]
      map.keySet.foreach(fieldName => {
        if (colsToDelete.contains(fieldName)) {
          colToDelete += fieldName
          valueSet += fieldName
          totalColumnsRemoved += 1
        }
      })
      if (colToDelete.size > 0) {
        totalRecordModified += 1
        if (!test) {
          //issue the delete command for the columns supplied
          pm.deleteColumns(guid, "occ", colsToDelete: _*)
        }
      }
      true
    }, startUuid, endUuid, 1000)
    logger.info("Finished cleanup for columns")
    logger.info("List of columns that have been removed from one or more records:")
    logger.info(valueSet.toList.toString())
    logger.info("total records changed: " + totalRecordModified + " out of " + totalRecords + ". " + totalColumnsRemoved + " columns were removed from cassandra")
  }

  /**
   * Removes the obsolete columns for the records that are contained in the supplied file.
   *
   * @param file
   * @param editTime
   * @param test
   */
  def removeObsoleteColumnsIncremental(file: java.io.File, editTime: Long, test: Boolean) {
    logger.info("Starting to remove the obsolete columns from an incremental file " + file.getAbsolutePath)
    var totalRecords = 0
    var totalRecordModified = 0
    var totalColumnsRemoved = 0
    val valueSet = new scala.collection.mutable.HashSet[String]
    val fullRecord = new FullRecord()
    file.foreachLine {
      line => {
        val (mod, rem) = removeRecordColumnsBasedOnTime(line, editTime, valueSet, fullRecord, test)
        totalColumnsRemoved += rem
        totalRecordModified += mod
        totalRecords += 1
      }
    }
    logger.info("Finished cleanup for columns")
    logger.info("List of columns that have been removed from one or more records:")
    logger.info(valueSet.toList.toString())
    logger.info("total records changed: " + totalRecordModified + " out of " + totalRecords + ". " + totalColumnsRemoved + " columns were removed from cassandra")
  }

  def removeRecordColumnsBasedOnTime(rowKey: String, editTime: Long, valueSet: scala.collection.mutable.HashSet[String], fullRecord: FullRecord, test: Boolean): (Int, Int) = {
    //check all the raw properties for the modified time.
    val timemap = Config.persistenceManager.getColumnsWithTimestamps(rowKey, "occ")
    val colToDelete = new ArrayBuffer[String]
    var totalColumnsRemoved = 0
    var totalRecordModified = 0
    val columnsToAlwaysKeep = Set("uuid", "originalSensitiveValues", "rowKey")

    //only interested in the raw values
    if (timemap.isDefined) {
      timemap.get.keySet.foreach(fieldName => {
        fieldName match {
          case it if (fullRecord.hasNestedProperty(it) && !columnsToAlwaysKeep.contains(it)) => {
            if (timemap.get.get(fieldName).get < editTime) {
              totalColumnsRemoved += 1
              colToDelete += fieldName
              valueSet += fieldName
            }
          }
          case _ => //ignore
        }
      })
    }
    if (colToDelete.size > 0) {
      totalRecordModified = 1
      if (!test) {
        //delete all the columns that were not updated
        Config.persistenceManager.deleteColumns(rowKey, "occ", colToDelete.toArray: _*)
      }
    }
    (totalRecordModified, totalColumnsRemoved)
  }

  def removeObsoleteColumns(dr: String, editTime: Long, start: Option[String], end: Option[String], test: Boolean = false) {
    logger.info("Starting to remove obsolete columns based on timestamps.")
    val startUuid = start.getOrElse(dr + "|")
    val endUuid = end.getOrElse(startUuid + "~")
    val fullRecord = new FullRecord
    val valueSet = new scala.collection.mutable.HashSet[String]
    var totalRecords = 0
    var totalRecordModified = 0
    var totalColumnsRemoved = 0
    val valuesToIgnore = Array("uuid", "originalSensitiveValues", "rowKey")
    Config.persistenceManager.pageOverSelect("occ", (guid, map) => {
      totalRecords += 1

      if (!map.contains("dateDeleted")) {
        //check all the raw properties for the modified time.
        val timemap = Config.persistenceManager.getColumnsWithTimestamps(guid, "occ")
        val colToDelete = new ArrayBuffer[String]

        //only interested in the raw values
        if (timemap.isDefined) {
          timemap.get.keySet.foreach(fieldName => {
            fieldName match {
              case it if (fullRecord.hasNestedProperty(fieldName) && !valuesToIgnore.contains(it)) => {
                if (timemap.get.get(fieldName).get < editTime) {
                  totalColumnsRemoved += 1
                  colToDelete += fieldName
                  valueSet += fieldName
                }
              }
              case _ => //ignore
            }
          })
        }
        if (colToDelete.size > 0) {
          totalRecordModified += 1
          if (!test) {
            //delete all the columns that were not updated
            Config.persistenceManager.deleteColumns(guid, "occ", colToDelete.toArray: _*)
          }
        }
      }
      true
    }, startUuid, endUuid, 1000, "rowKey", "dateDeleted")
    logger.info("Finished cleanup for columns")
    logger.info("List of columns that have been removed from one or more records:")
    logger.info(valueSet.toList.toString())
    logger.info("total records changed: " + totalRecordModified + " out of " + totalRecords + ". " + totalColumnsRemoved + " columns were removed from cassandra")
  }


  def modifyRecord(dr: String, lastDate: java.util.Date, start: Option[String], end: Option[String], test: Boolean = false) {
    val startUuid = start.getOrElse(dr + "|")
    val endUuid = end.getOrElse(dr + "|~")
    val deleteTime = org.apache.commons.lang.time.DateFormatUtils.format(new java.util.Date, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    var totalRecords = 0
    var deleted = 0
    var reinstate = 0

    Config.persistenceManager.pageOverSelect("occ", (guid, map) => {
      totalRecords += 1
      //check to see if lastModifiedDate and dateDeleted settings require changes to
      val lastModified = DateParser.parseStringToDate(map.getOrElse("lastModifiedTime", ""));
      val dateDeleted = DateParser.parseStringToDate(map.getOrElse("dateDeleted", ""));
      if (lastModified.isDefined) {
        if (lastModified.get.before(lastDate)) {
          //we need to mark this record as deleted if it is not already
          if (dateDeleted.isEmpty) {
            deleted += 1
            if (!test) {
              Config.occurrenceDAO.setDeleted(guid, true, Some(deleteTime))
            }
          }
        } else {
          //the record is current set the record undeleted if it was deleted previously
          if (dateDeleted.isDefined) {
            reinstate += 1
            if (!test) {
              Config.occurrenceDAO.setDeleted(guid, false)
            }
          }
        }
      } else {
        //need to delete it anyway because there was no last modified for the record
        deleted += 1
        if (!test) {
          Config.occurrenceDAO.setDeleted(guid, true, Some(deleteTime))
        }
      }
      true
    }, startUuid, endUuid, 1000, "rowKey", "uuid", "lastModifiedTime", "dateDeleted")
    println("Finished cleanup for rows")
    println("Records checked: " + totalRecords + " Records deleted: " + deleted + " Records reinstated: " + reinstate)
  }

  /**
   * removes the deleted record from the occ column family and places it in the dellog column family
   * when last modified time occurs before lastDate
   */
  def removeDeletedRecords(dr: String, lastDate: java.util.Date, start: Option[String], end: Option[String], test: Boolean = false) {
    val occDao = Config.occurrenceDAO
    var count = 0
    var totalRecords = 0
    val startUuid = start.getOrElse(dr + "|")
    val endUuid = end.getOrElse(startUuid + "~")
    Config.persistenceManager.pageOverSelect("occ", (guid, map) => {
      totalRecords += 1
      val delete = map.getOrElse(FullRecordMapper.deletedColumn, "false")

      //check to see if lastModifiedDate is candidate for move
      val lastModified = DateParser.parseStringToDate(map.getOrElse("lastModifiedTime", ""))
      if (lastModified.isDefined) {
        if (lastModified.get.before(lastDate)) {
          if ("true".equals(delete)) {
            if (!test) {
              occDao.delete(guid, false, true)
            }
            count = count + 1
          }
        }
      }
      if (totalRecords % 1000 == 0 && !test) {
        logger.info("Deleted " + count + " records out of " + totalRecords)
        logger.info("Last key checked: " + guid)
      }
      true
    }, startUuid, endUuid, 1000, "rowKey", "uuid", FullRecordMapper.deletedColumn, "lastModifiedTime")

    println("Finished moving deleted occ rows to dellog")
    println("Records deleted: " + count)
  }

  def removeRawRecordColumnsNotInListByDate(dr: String, colsToKeep: Array[String], editTime: Long, start: Option[String], end: Option[String], test: Boolean = false) {
    logger.info("Starting to remove columns based on timestamps and not in colsToKeep: " + colsToKeep.toList)
    val startUuid = start.getOrElse(dr + "|")
    val endUuid = end.getOrElse(startUuid + "~")
    val fullRecord = new FullRecord
    val valueSet = new scala.collection.mutable.HashSet[String]
    var totalRecords = 0
    var totalRecordModified = 0
    var totalColumnsRemoved = 0
    val valuesToIgnore = Array("uuid", "originalSensitiveValues", "rowKey")
    Config.persistenceManager.pageOverAll("occ", (guid, map) => {
      totalRecords += 1

      if (!map.contains("dateDeleted")) {
        //check all the raw properties for the modified time.
        val timemap = Config.persistenceManager.getColumnsWithTimestamps(guid, "occ")
        val colToDelete = new ArrayBuffer[String]

        //only interested in the raw values
        if (timemap.isDefined) {
          timemap.get.keySet.foreach(fieldName => {
            fieldName match {
              case it if (fullRecord.hasNestedProperty(fieldName) && !valuesToIgnore.contains(fieldName) &&
                !colsToKeep.contains(fieldName)) => {
                if (timemap.get.get(fieldName).get < editTime) {
                  totalColumnsRemoved += 1
                  colToDelete += fieldName
                  valueSet += fieldName
                }
              }
              case _ => //ignore
            }
          })
        }
        if (colToDelete.size > 0) {
          totalRecordModified += 1
          if (!test) {
            //delete all the columns that were not updated
            Config.persistenceManager.deleteColumns(guid, "occ", colToDelete.toArray: _*)
          }
        }
      }
      true
    }, startUuid, endUuid, 1000)
    logger.info("Finished cleanup for columns")
    logger.info("List of columns that have been removed from one or more records:")
    logger.info(valueSet.toList.toString())
    logger.info("total records changed: " + totalRecordModified + " out of " + totalRecords + ". " + totalColumnsRemoved + " columns were removed from cassandra")
  }

  def removeSpecifiedColumnsByDate(dr: String, colsToDelete: Array[String], editTime: Long, start: Option[String], end: Option[String], test: Boolean = false) {
    logger.info("Starting to remove columns based on timestamps and in colsToDelete: " + colsToDelete.toList)
    val startUuid = start.getOrElse(dr + "|")
    val endUuid = end.getOrElse(startUuid + "~")
    val fullRecord = new FullRecord
    val valueSet = new scala.collection.mutable.HashSet[String]
    var totalRecords = 0
    var totalRecordModified = 0
    var totalColumnsRemoved = 0
    val valuesToIgnore = Array("uuid", "originalSensitiveValues", "rowKey")
    Config.persistenceManager.pageOverAll("occ", (guid, map) => {
      totalRecords += 1

      if (!map.contains("dateDeleted")) {
        //check all the raw properties for the modified time.
        val timemap = Config.persistenceManager.getColumnsWithTimestamps(guid, "occ")
        val colToDelete = new ArrayBuffer[String]

        //only interested in the raw values
        if (timemap.isDefined) {
          timemap.get.keySet.foreach(fieldName => {
            if (colsToDelete.contains(fieldName) && timemap.get.get(fieldName).get < editTime) {
              totalColumnsRemoved += 1
              colToDelete += fieldName
              valueSet += fieldName
            }
          })
        }
        if (colToDelete.size > 0) {
          totalRecordModified += 1
          if (!test) {
            //delete all the columns that were not updated
            Config.persistenceManager.deleteColumns(guid, "occ", colToDelete.toArray: _*)
          }
        }
      }
      true
    }, startUuid, endUuid, 1000)
    logger.info("Finished cleanup for columns")
    logger.info("List of columns that have been removed from one or more records:")
    logger.info(valueSet.toList.toString())
    logger.info("total records changed: " + totalRecordModified + " out of " + totalRecords + ". " + totalColumnsRemoved + " columns were removed from cassandra")
  }
}

