package au.org.ala.biocache.processor

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap
import java.util.{Date, UUID}

import au.org.ala.biocache
import au.org.ala.biocache._
import au.org.ala.biocache.dao.OccurrenceDAO
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.model.{FullRecord, Processed, QualityAssertion, Versions}
import org.slf4j.LoggerFactory
import org.codehaus.jackson.map.ObjectMapper

import scala.collection.mutable

/**
 * Runnable for starting record processing.
 *
 * <ol>
 * <li> Classification matching
 * 	- include a flag to indicate record hasnt been matched to NSLs
 * </li>
 *
 * <li> Parse locality information
 * 	- "Vic" -> Victoria
 * </li>
 *
 * <li> Point matching
 * 	- parse latitude/longitude
 * 	- retrieve associated point mapping
 * 	- check state supplied to state point lies in
 * 	- marine/non-marine/limnetic (need a webservice from BIE)
 * </li>
 *
 * <li> Type status normalization
 * 	- use GBIF's vocabulary
 * </li>
 *
 * <li> Date parsing
 * 	- date validation
 * 	- support for date ranges
 * </li>
 *
 * <li> Collectory lookups for attribution chain </li>
 *
 * </ol>
 *
 * Tests to conform to: http://bit.ly/eqSiFs
 */
class RecordProcessor {

  val logger = LoggerFactory.getLogger(classOf[RecordProcessor])
  //The time that the processing started - used to populate lastProcessed
  val processTime = org.apache.commons.lang.time.DateFormatUtils.format(new java.util.Date, "yyyy-MM-dd'T'HH:mm:ss'Z'")
  logger.info("RecordProcessor is instantiated. processTime:" + processTime)
  val duplicates = List("D", "D1", "D2")

  val processTimings: ConcurrentHashMap[String, AtomicLong] = new ConcurrentHashMap[String, AtomicLong]()

  def getProcessTimings = processTimings

  /**
   * Process a record, adding metadata and records quality systemAssertions.
   * This version passes the original to optimise updates.
   *
   * When it is a batch, the record to be updated is returned for batch commits with writeProcessBatch.
   *
   * When it is a firstLoad, there will be no offline assertions 
   */
  def processRecord(raw: FullRecord, currentProcessed: FullRecord, batch: Boolean = false, firstLoad: Boolean = false,
                    processors: Option[String] = None) : Map[String, Object] = {
    val objectMapper = if (logger.isTraceEnabled) {
      Some(new ObjectMapper)
    } else {
      None
    }
    try {
      val guid = raw.rowKey
      val occurrenceDAO = Config.getInstance(classOf[OccurrenceDAO]).asInstanceOf[OccurrenceDAO]
      //NC: Changed so that a processed record only contains values that have been processed.
      val processed = raw.createNewProcessedRecord
      //var assertions = new ArrayBuffer[QualityAssertion]
      var assertions = new scala.collection.mutable.HashMap[String, Array[QualityAssertion]]

      //run each processor in the specified order
      Processors.foreach { processor =>
        // when processing a new record (firstLoad==true), there is no need to include offline processing
        if (!firstLoad || !processor.getName.equals("offline")) {
          val start = System.nanoTime()
          try {
            if (processors.isEmpty || processors.get.contains(processor.getName)) {
              assertions += (processor.getName -> processor.process(guid, raw, processed, Some(currentProcessed)))
              if (logger.isTraceEnabled()) {
                logger.trace("Processing completed for '" + processor.getName + "' for record " + guid)
                logger.trace(objectMapper.get.writeValueAsString(processed))
              }
            } else {
              assertions += (processor.getName -> processor.skip(guid, raw, processed, Some(currentProcessed)))
              if (logger.isTraceEnabled()) {
                logger.trace("Processing skipped for '" + processor.getName + "' for record " + guid)
              }
            }
          } catch {
            case e: Exception => {
              logger.warn("Non-fatal error processing record: " + raw.rowKey + ", processorName: " + processor.getName + ", error: " + e.getMessage(), e)
            }
          } finally {
            processTimings.getOrDefault(processor.getName, new AtomicLong(0)).addAndGet((System.nanoTime() - start))
          }
        }
      }

      //mark the processed time
      processed.lastModifiedTime = processTime
      //store the occurrence
      val systemAssertions = Some(assertions.toMap)

      if (batch) {
        Map("rowKey" -> guid,
          "oldRecord" -> currentProcessed,
          "newRecord" -> processed,
          "assertions" -> systemAssertions,
          "version" -> Processed
        )
      } else {
        val startPersist = System.nanoTime()
        occurrenceDAO.updateOccurrence(guid, currentProcessed, processed, systemAssertions, Processed)
        processTimings.getOrDefault("persist", new AtomicLong(0)).addAndGet((System.nanoTime() - startPersist))
        null
      }
    } catch {
      case e: Exception => {
        logger.error("Error processing record: " + raw.rowKey + ", " + e.getMessage(), e)
        null
      }
    }
  }

  /**
   * commits batched records returned by processRecord
   *
   * @param batch
   */
  def writeProcessBatch(batch: List[Map[String, Object]]) = {
    val occurrenceDAO = Config.getInstance(classOf[OccurrenceDAO]).asInstanceOf[OccurrenceDAO]

    var retries = 0
    var processedOK = false
    while (!processedOK && retries < 6) {
      try {
        occurrenceDAO.updateOccurrenceBatch(batch)
        processedOK = true
      } catch {
        case e: Exception => {
          logger.error("Error processing record batch with length: '" + batch.length + "',  sleeping for 20 secs before retries", e)
          Thread.sleep(20000)
          retries += 1
        }
      }
    }
  }

  /**
   * Process a record, adding metadata and records quality systemAssertions
   */
  def processRecord(raw:FullRecord) : (FullRecord, Map[String, Array[QualityAssertion]]) = {

    //NC: Changed so that a processed record only contains values that have been processed.
    val processed = raw.createNewProcessedRecord
    val assertions = new scala.collection.mutable.HashMap[String, Array[QualityAssertion]]

    Processors.foreach(processor => {
      if (logger.isDebugEnabled) {
        logger.debug("Running processor " + processor.getName)
      }
      assertions += (processor.getName -> processor.process(raw.rowKey, raw, processed))
    })

    processed.lastModifiedTime = org.apache.commons.lang.time.DateFormatUtils.format(
      new Date, "yyyy-MM-dd'T'HH:mm:ss'Z'")

    //return the processed version and assertions
    (processed, assertions.toMap)
  }

  /**
   * Process a record, adding metadata and records quality systemAssertions
   */
  def processRecordAndUpdate(raw:FullRecord){

    val (processed, assertions) = processRecord(raw)
    val systemAssertions = Some(assertions)
    //mark the processed time
    processed.asInstanceOf[FullRecord].lastModifiedTime = processTime
    //store the occurrence
    Config.occurrenceDAO.updateOccurrence(raw.rowKey, processed, systemAssertions, Processed)
  }

  /**
    * Add a record. This is only used by sandbox uploads.
    *
    * @param dataResourceUid
    * @param properties
    * @return
    */
  def addRecord(dataResourceUid:String, properties:Map[String,String]) : String = {
    val uuid =  UUID.randomUUID().toString
    val raw = FullRecordMapper.createFullRecord(uuid, properties,Versions.RAW)
    raw.attribution.dataResourceUid = dataResourceUid
    Config.occurrenceDAO.updateOccurrence(raw.rowKey, raw, None, Versions.RAW)
    val downloaded = Config.occurrenceDAO.downloadMedia(raw)
    if (downloaded){
      Config.occurrenceDAO.updateOccurrence(raw.rowKey, raw, None, Versions.RAW)
    }
    uuid
  }
}
