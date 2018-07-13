package au.org.ala.biocache

import au.org.ala.biocache.caches.{SensitivityDAO}
import au.org.ala.biocache.model.FullRecord
import au.org.ala.biocache.processor.{ SensitivityProcessor}
import org.apache.commons.lang3.StringUtils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * Test for the SDS with non Australian data to ensure re-usability.
  */
@RunWith(classOf[JUnitRunner])
class ScotlandSensitivityTest extends ScottishConfigFunSuite {

  test("Idempotent test - Numenius phaeopus in 55.9486° N, 3.2008° W"){
    pm.clear

    val raw = new FullRecord
    val processed = new FullRecord

    raw.rowKey = "1"
    processed.rowKey = "1"

    raw.classification.scientificName = "Hericium cirrhatum"
    processed.classification.taxonConceptID = "1234"
    processed.classification.scientificName = "Hericium cirrhatum"
    raw.location.decimalLatitude = "55.9486"
    raw.location.decimalLongitude = "-3.2008"
    raw.location.stateProvince = "England"
    raw.location.locality = "Up the Gyhll"
    processed.location.stateProvince = "England"

    SensitivityDAO.addToCache("Hericium cirrhatum", "1234", true)

    (new SensitivityProcessor).process("test", raw, processed)

    expectResult(true){processed.occurrence.dataGeneralizations != null}
    expectResult(true){processed.occurrence.dataGeneralizations.contains("generalised")}
    expectResult(false){processed.occurrence.dataGeneralizations.contains("already generalised")}

    raw.location.stateProvince = "England"
    processed.location.stateProvince = "England"

    (new SensitivityProcessor).process("test", raw, processed)

    expectResult(true){processed.occurrence.dataGeneralizations != null}
    expectResult(true){processed.occurrence.dataGeneralizations.contains("generalised")}
    expectResult(false){processed.occurrence.dataGeneralizations.contains("already generalised")}
  }

  test("Lutra lutra in 55.9486° N, 3.2008° W"){
    pm.clear

    val raw = new FullRecord
    val processed = new FullRecord
    processed.classification.taxonConceptID = "1234"
    raw.classification.scientificName = "Lutra lutra"
    raw.location.stateProvince = "Scotland"
    processed.location.stateProvince = "Scotland"
    raw.location.decimalLatitude = "55.9486"
    raw.location.decimalLongitude = "-3.2008"
    SensitivityDAO.addToCache("Lutra lutra", "1234", true)

    (new SensitivityProcessor).process("test", raw, processed)
    expectResult(true) {
      StringUtils.isNotBlank(processed.occurrence.dataGeneralizations)
    }
  }

  test("Lutra lutra in 56.65681, -3.15419"){
    pm.clear

    val raw = new FullRecord
    val processed = new FullRecord
    processed.classification.taxonConceptID = "1234"
    raw.classification.scientificName = "Lutra lutra"
    raw.location.stateProvince = "Scotland"
    processed.location.stateProvince = "Scotland"
    raw.location.decimalLatitude = "56.65681"
    raw.location.decimalLongitude = "-3.15419"
    SensitivityDAO.addToCache("Lutra lutra", "1234", true)

    (new SensitivityProcessor).process("test", raw, processed)
    expectResult(true) {
      StringUtils.isNotBlank(processed.occurrence.dataGeneralizations)
    }
  }
}
