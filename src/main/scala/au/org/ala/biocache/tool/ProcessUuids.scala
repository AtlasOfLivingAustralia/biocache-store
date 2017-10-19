package au.org.ala.biocache.tool

import java.io.{BufferedReader, InputStreamReader}

import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.model.Processed
import au.org.ala.biocache.processor.RecordProcessor
import au.org.ala.biocache.util.OptionParser
import org.codehaus.jackson.map.ObjectMapper
import org.slf4j.LoggerFactory

/**
 * Utility for processing list of records specified by a CSV uuids string.
 */
object ProcessUuids extends Tool {

  val logger = LoggerFactory.getLogger("ProcessUuids")

  def cmd = "process-uuids"
  def desc = "Process a list of records specified by a comma separated uuid string"

  def main(args: Array[String]) {

    var uuids = ""

    val parser = new OptionParser(help) {
      arg("uuids", "UUIDs", {
        v: String => uuids = v
      })
    }

    if (parser.parse(args)) {
      processRecords(uuids)
    }
  }

  def processRecords(uuids: String) {
    val processor = new RecordProcessor
    uuids.split(",").foreach(uuid => {
      var records = Config.occurrenceDAO.getAllVersionsByRowKey(uuid)
      if (!records.isEmpty) {
        logger.info("Processing record.....")
        processor.processRecord(records.get(0), records.get(1))
        val processedRecord = Config.occurrenceDAO.getByRowKey(records.get(1).rowKey, Processed)
        val objectMapper = new ObjectMapper
        if (!processedRecord.isEmpty)
          logger.info(objectMapper.writeValueAsString(processedRecord.get))
        else
          logger.info("Record not found")
      } else {
        logger.info("UUID or row key not stored....")
      }
    })
    print("\n\nSupply a Row Key for a record: ")
  }

  def readStdIn = (new BufferedReader(new InputStreamReader(System.in))).readLine.trim
}