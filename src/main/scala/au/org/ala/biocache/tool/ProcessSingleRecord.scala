package au.org.ala.biocache.tool

import java.io.{InputStreamReader, BufferedReader}
import org.codehaus.jackson.map.ObjectMapper
import org.slf4j.LoggerFactory
import au.org.ala.biocache.Config
import au.org.ala.biocache.model.{Processed, Versions}
import au.org.ala.biocache.util.OptionParser
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.processor.RecordProcessor

/**
 * Utility for processing a single record. Useful for testing purposes.
 */
object ProcessSingleRecord extends Tool {

  val logger = LoggerFactory.getLogger("ProcessSingleRecord")

  def cmd = "process-single"
  def desc = "Process a single record"

  def main(args: Array[String]) {

    var uuid = ""

    val parser = new OptionParser(help) {
      arg("uuid", "record UUID", {
        v: String => uuid = v
      })
    }

    if (parser.parse(args)) {
      processRecord(uuid)
    }
  }

  def processRecord(uuid: String) {
    val processor = new RecordProcessor
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
    print("\n\nSupply a Row Key for a record: ")
  }

  def readStdIn = (new BufferedReader(new InputStreamReader(System.in))).readLine.trim
}