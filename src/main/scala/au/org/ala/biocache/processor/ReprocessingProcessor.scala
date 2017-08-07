package au.org.ala.biocache.processor

import au.org.ala.biocache.model.{FullRecord, QualityAssertion}
import org.slf4j.LoggerFactory

/**
  * A processor of attribution information.
  */
class ReprocessingProcessor extends Processor {

  val logger = LoggerFactory.getLogger("ReprocessingProcessor")

  /**
    * Retain lastProcessed processed values that are not added by other Processors.
    *
    * - Sampling (el and cl)
    */
  def process(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {

    //copy el and cl fields when processed location is unchanged
    if (lastProcessed.isDefined && lastProcessed.get.location.decimalLongitude == processed.location.decimalLongitude &&
      lastProcessed.get.location.decimalLatitude == processed.location.decimalLatitude &&
      ((lastProcessed.get.el != null && lastProcessed.get.el.size() > 0) ||
        (lastProcessed.get.cl != null && lastProcessed.get.cl.size() > 0))) {
      processed.el = lastProcessed.get.el
      processed.cl = lastProcessed.get.cl
    }

    Array()
  }

  def skip(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {
    //run process
    process(guid, raw, processed, lastProcessed)
  }

  def getName = "reprocessing"
}