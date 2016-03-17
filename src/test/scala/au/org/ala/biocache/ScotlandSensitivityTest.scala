package au.org.ala.biocache

import au.org.ala.biocache.caches.SpatialLayerDAO
import au.org.ala.biocache.model.FullRecord
import au.org.ala.biocache.processor.LocationProcessor
import org.apache.commons.lang3.StringUtils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * Created by mar759 on 10/03/2016.
  */
@RunWith(classOf[JUnitRunner])
class ScotlandSensitivityTest extends ScottishConfigFunSuite {

  test("Numenius phaeopus in 55.9486° N, 3.2008° W"){
    val raw = new FullRecord
    val processed = new FullRecord
    raw.classification.scientificName = "Numenius phaeopus"
    raw.location.decimalLatitude = "55.9486"
    raw.location.decimalLongitude = "-3.2008"

    (new LocationProcessor).process("test", raw, processed)
    expectResult(true) {
      println (processed.occurrence.dataGeneralizations)
      println (processed.location.decimalLatitude + " " + processed.location.decimalLongitude)
      StringUtils.isNotBlank(processed.occurrence.dataGeneralizations)
    }
  }

  test("Lutra lutra in 55.9486° N, 3.2008° W"){
    val raw = new FullRecord
    val processed = new FullRecord
    raw.classification.scientificName = "Lutra lutra"
    raw.location.decimalLatitude = "55.9486"
    raw.location.decimalLongitude = "-3.2008"

    (new LocationProcessor).process("test", raw, processed)
    expectResult(true) {
      println (processed.occurrence.dataGeneralizations)
      println (processed.location.decimalLatitude + " " + processed.location.decimalLongitude)
      StringUtils.isNotBlank(processed.occurrence.dataGeneralizations)
    }
  }

  test("Lutra lutra in 56.65681, -3.15419"){
    val raw = new FullRecord
    val processed = new FullRecord
    raw.classification.scientificName = "Lutra lutra"
    raw.location.decimalLatitude = "56.65681"
    raw.location.decimalLongitude = "-3.15419"

    (new LocationProcessor).process("test", raw, processed)
    expectResult(true) {
      println (processed.occurrence.dataGeneralizations)
      println (processed.location.decimalLatitude + " " + processed.location.decimalLongitude)
      StringUtils.isNotBlank(processed.occurrence.dataGeneralizations)
    }
  }
}
