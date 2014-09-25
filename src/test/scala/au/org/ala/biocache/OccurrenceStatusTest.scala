package au.org.ala.biocache

import au.org.ala.biocache.model.{QualityAssertion, FullRecord}
import au.org.ala.biocache.processor.MiscellaneousProcessor
import au.org.ala.biocache.vocab.AssertionCodes
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class OccurrenceStatusTest extends ConfigFunSuite {

  test("present value supplied"){

    val raw = new FullRecord
    raw.occurrence.occurrenceStatus = "Present"
    val processed = new FullRecord
    val assertions = new ArrayBuffer[QualityAssertion]()

    val mp = new MiscellaneousProcessor
    mp.processOccurrenceStatus(raw, processed, assertions)

    expectResult(true){ assertions.isEmpty }
    expectResult(true){ processed.occurrence.occurrenceStatus == "present" }
  }

  test("absent value supplied"){
    val raw = new FullRecord
    raw.occurrence.occurrenceStatus = "Absent"
    val processed = new FullRecord
    val assertions = new ArrayBuffer[QualityAssertion]()

    val mp = new MiscellaneousProcessor
    mp.processOccurrenceStatus(raw, processed, assertions)

    expectResult(true){ assertions.isEmpty }
    expectResult(true){ processed.occurrence.occurrenceStatus == "absent" }
  }

  test("absence value supplied"){
    val raw = new FullRecord
    raw.occurrence.occurrenceStatus = "Absence"
    val processed = new FullRecord
    val assertions = new ArrayBuffer[QualityAssertion]()

    val mp = new MiscellaneousProcessor
    mp.processOccurrenceStatus(raw, processed, assertions)

    expectResult(true){ assertions.isEmpty }
    expectResult(true){ processed.occurrence.occurrenceStatus == "absent" }
  }

  test("no value supplied - assume present, add a warning"){

    val raw = new FullRecord
    val processed = new FullRecord
    val assertions = new ArrayBuffer[QualityAssertion]()

    val mp = new MiscellaneousProcessor
    mp.processOccurrenceStatus(raw, processed, assertions)

    expectResult(true){ assertions.size == 1 }
    expectResult(true){ processed.occurrence.occurrenceStatus == "present" }
    expectResult(AssertionCodes.ASSUMED_PRESENT_OCCURRENCE_STATUS.code){ assertions(0).code}
  }

  test("unrecognised value supplied - set to unknown, add a warning"){

    val raw = new FullRecord
    raw.occurrence.occurrenceStatus = "12321321321"
    val processed = new FullRecord
    val assertions = new ArrayBuffer[QualityAssertion]()

    val mp = new MiscellaneousProcessor
    mp.processOccurrenceStatus(raw, processed, assertions)

    expectResult(true){ processed.occurrence.occurrenceStatus == "unknown" }
    expectResult(true){ assertions.size == 1 }
    expectResult( AssertionCodes.UNRECOGNISED_OCCURRENCE_STATUS.code){ assertions(0).code}
  }
}
