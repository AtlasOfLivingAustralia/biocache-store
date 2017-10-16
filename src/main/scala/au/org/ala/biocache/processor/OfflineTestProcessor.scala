package au.org.ala.biocache.processor

import au.org.ala.biocache.model.{FullRecord, QualityAssertion}
import au.org.ala.biocache.vocab.AssertionCodes

/**
  * A processor to ensure that the offline test that were performed get recorded correctly
  */
class OfflineTestProcessor extends Processor {

  def process(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {

    if (lastProcessed.isDefined) {
      //get the current system assertions
      val currentProcessed = lastProcessed.get
      val systemAssertions = currentProcessed.findAssertions()
      val offlineAssertions = systemAssertions.filter(sa => AssertionCodes.offlineAssertionCodes.contains(AssertionCodes.getByCode(sa.code).getOrElse(AssertionCodes.GEOSPATIAL_ISSUE)))
      processed.occurrence.outlierForLayers = currentProcessed.occurrence.outlierForLayers
      processed.occurrence.duplicationStatus = currentProcessed.occurrence.duplicationStatus
      processed.occurrence.duplicationType = currentProcessed.occurrence.duplicationType
      processed.occurrence.associatedOccurrences = currentProcessed.occurrence.associatedOccurrences
      processed.location.distanceOutsideExpertRange = currentProcessed.location.distanceOutsideExpertRange
      processed.queryAssertions = currentProcessed.queryAssertions
      offlineAssertions.toArray
    } else {
      //assume that the assertions were not tested
      Array()
    }
  }

  def skip(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {
    //low process overhead, no need to skip
    process(guid, raw, processed, lastProcessed)
  }

  def getName = "offline"
}
