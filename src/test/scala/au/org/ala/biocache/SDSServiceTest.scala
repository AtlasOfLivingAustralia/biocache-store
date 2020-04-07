package au.org.ala.biocache

import au.org.ala.biocache.caches.{SensitivityDAO, SpatialLayerDAO}
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.model.{FullRecord, Versions}
import au.org.ala.biocache.processor.{LocationProcessor, SensitivityProcessor}
import au.org.ala.biocache.vocab.AssertionCodes
import org.apache.commons.lang.StringUtils
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.junit.JUnitRunner
import org.junit.Ignore

/**
 * Performs some tests against the SDS service.
 * Generally turned off, turn on if you want to test against an actual server
 */
@RunWith(classOf[JUnitRunner])
@Ignore
class SDSServiceTest extends FunSuite {
  /** Enable this test to test the actual SDS interface */
  test("State based sensitivity via service") {
    val configFilePath = System.getProperty("user.dir") +  "/src/test/resources/biocache-sds-test-config.properties"
    System.setProperty("biocache.config", configFilePath)

    SpatialLayerDAO.addToCache(146.921099, -31.2532183, Map("cl10925" -> "New South Wales"))

    val raw = new FullRecord
    val processed = new FullRecord
    raw.classification.scientificName = "Diuris disposita"
    processed.classification.scientificName = "Diuris disposita"
    processed.classification.taxonConceptID = "https://id.biodiversity.org.au/name/apni/117435"
    processed.classification.taxonRankID = "7000"
    raw.location.decimalLatitude = "-31.2532183"
    raw.location.decimalLongitude = "146.921099"
    raw.location.stateProvince = "NSW"
    raw.location.locality = "My test locality"

    val rawMap = scala.collection.mutable.Map[String, String]()
    raw.objectArray.foreach { poso =>
      val map = FullRecordMapper.mapObjectToProperties(poso, Versions.RAW)
      rawMap ++= map
    }
    raw.setRawFieldsWithMapping(rawMap)

    (new LocationProcessor).process("test", raw, processed)
    (new SensitivityProcessor).process("test", raw, processed)
    expectResult(true) {
      StringUtils.isNotBlank(processed.occurrence.dataGeneralizations)
    }
  }
}
