package au.org.ala.biocache

import au.org.ala.biocache.model.FullRecord
import au.org.ala.biocache.processor.IdentificationQualifierProcessor
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
 * Tests for IdentificationQualifierProcessor
 */
@RunWith(classOf[JUnitRunner])
class IdentificationQualifierTests extends ConfigFunSuite {

  test("Test IdentificationQualifierProcessor process"){

    val iqp = new IdentificationQualifierProcessor()
    val raw = new FullRecord()
    val processed = new FullRecord()

    raw.identification.identificationQualifier = "?"
    iqp.process("",raw, processed)
    expectResult(true) { processed.identification.identificationQualifier == "Uncertain" }

    raw.identification.identificationQualifier = "? abc"
    iqp.process("",raw, processed)
    expectResult(true) { processed.identification.identificationQualifier == "Uncertain" }

    raw.identification.identificationQualifier = "sp."
    iqp.process("",raw, processed)
    expectResult(true) { processed.identification.identificationQualifier == "Uncertain" }

    raw.identification.identificationQualifier = "sp. abc"
    iqp.process("",raw, processed)
    expectResult(true) { processed.identification.identificationQualifier == "Uncertain" }

    raw.identification.identificationQualifier = "not confirmed"
    iqp.process("",raw, processed)
    expectResult(true) { processed.identification.identificationQualifier == "Uncertain" }

    raw.identification.identificationQualifier = "not confirm"
    iqp.process("",raw, processed)
    expectResult(true) { processed.identification.identificationQualifier == "Uncertain" }

    raw.identification.identificationQualifier = "certain"
    iqp.process("",raw, processed)
    expectResult(true) { processed.identification.identificationQualifier == "Certain" }

    raw.identification.identificationQualifier = ""
    iqp.process("",raw, processed)
    expectResult(true) { processed.identification.identificationQualifier == "Not provided" }

    raw.identification.identificationQualifier = "abc as"
    iqp.process("",raw, processed)
    expectResult(true) { processed.identification.identificationQualifier == "Not recognised" }
  }
}