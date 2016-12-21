package au.org.ala.biocache.processor

import au.org.ala.biocache.vocab.AssertionCodes

/**
 * Singleton that maintains the workflow of processors
 */
object Processors {

  import AssertionCodes._
  
  def foreach(proc: Processor => Unit) = processorMap.values.foreach(proc)

  //need to preserve the ordering of the Processors so that the default values are populated first
  //also classification must be executed before location
  val processorMap = scala.collection.mutable.LinkedHashMap(
    "DEFAULT" -> new DefaultValuesProcessor,
    "IMAGE" -> new MiscellaneousProcessor,
    "OFFLINE" -> new OfflineTestProcessor,
    "ATTR" -> new AttributionProcessor,
    "CLASS" -> new ClassificationProcessor,
    "BOR" -> new BasisOfRecordProcessor,
    "EVENT" -> new EventProcessor,
    "LOC" -> new LocationProcessor,
    "SENSITIVE" -> new SensitivityProcessor,
    "TS" -> new TypeStatusProcessor,
    "IQ" -> new IdentificationQualifierProcessor
  )

  //TODO A better way to do this. Maybe need to group QA failures by issue type instead of phase.
  //Can't change until we are able to reprocess the complete set records.
  def getProcessorForError(code: Int): String = code match {
    case c if c == INFERRED_DUPLICATE_RECORD.code || c == DETECTED_OUTLIER.code || c == SPECIES_OUTSIDE_EXPERT_RANGE.code => "offline"
    case c if c >= geospatialBounds._1 && c < geospatialBounds._2 => "loc"
    case c if c >= taxonomicBounds._1 && c < taxonomicBounds._2 => "class"
    case c if c == MISSING_BASIS_OF_RECORD.code || c == BADLY_FORMED_BASIS_OF_RECORD.code => "bor"
    case c if c == UNRECOGNISED_TYPESTATUS.code => "type"
    case c if c == UNRECOGNISED_COLLECTIONCODE.code || c == UNRECOGNISED_INSTITUTIONCODE.code => "attr"
    case c if c == INVALID_IMAGE_URL.code => "image"
    case c if c >= temporalBounds._1 && c < temporalBounds._2 => "event"
    case _ => ""
  }
}
