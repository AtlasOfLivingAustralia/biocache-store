package au.org.ala.biocache.processor

import au.org.ala.biocache.model.{FullRecord, QualityAssertion}
import au.org.ala.biocache.vocab.{AssertionCodes, AssertionStatus, BasisOfRecord}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * A processor of basis of record information.
  */
class BasisOfRecordProcessor extends Processor {

  val logger = LoggerFactory.getLogger("BasisOfRecordProcessor")

  import AssertionCodes._
  import AssertionStatus._

  /**
    * Process basis of record
    */
  def process(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {

    if (raw.occurrence.basisOfRecord == null || raw.occurrence.basisOfRecord.isEmpty) {
      if (processed.occurrence.basisOfRecord != null && !processed.occurrence.basisOfRecord.isEmpty)
        Array[QualityAssertion]() //NC: When using default values we are not testing against so the QAs don't need to be included.
      else //add a quality assertion
        Array(QualityAssertion(MISSING_BASIS_OF_RECORD, "Missing basis of record"))
    } else {
      val term = BasisOfRecord.matchTerm(raw.occurrence.basisOfRecord)
      if (term.isEmpty) {
        //add a quality assertion
        logger.debug("[QualityAssertion] " + guid + ", unrecognised BoR: " + guid + ", BoR:" + raw.occurrence.basisOfRecord)
        Array(QualityAssertion(BADLY_FORMED_BASIS_OF_RECORD, "Unrecognised basis of record"), QualityAssertion(MISSING_BASIS_OF_RECORD, PASSED))
      } else {
        processed.occurrence.basisOfRecord = term.get.canonical
        Array[QualityAssertion](QualityAssertion(MISSING_BASIS_OF_RECORD, PASSED), QualityAssertion(BADLY_FORMED_BASIS_OF_RECORD, PASSED))
      }
    }
  }

  def skip(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {
    var assertions = new ArrayBuffer[QualityAssertion]

    if (lastProcessed.isDefined) {
      assertions ++= lastProcessed.get.findAssertions(Array(MISSING_BASIS_OF_RECORD.code, BADLY_FORMED_BASIS_OF_RECORD.code))

      //update the details from lastProcessed
      processed.occurrence.basisOfRecord = lastProcessed.get.occurrence.basisOfRecord
    }

    assertions.toArray
  }

  def getName() = "bor"
}