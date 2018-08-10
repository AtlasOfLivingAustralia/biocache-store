package au.org.ala.biocache

import au.org.ala.biocache.caches.{SensitivityDAO, SpatialLayerDAO}
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.model.{FullRecord, Versions}
import au.org.ala.biocache.processor.{EventProcessor, LocationProcessor, SensitivityProcessor}
import au.org.ala.biocache.vocab.AssertionCodes
import org.apache.commons.lang.StringUtils
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import au.org.ala.biocache.processor.{EventProcessor, LocationProcessor, SensitivityProcessor}
import au.org.ala.biocache.model.FullRecord

/**
 * Performs some Location Processing tests
 */
@RunWith(classOf[JUnitRunner])
class ProcessLocationTest extends ConfigFunSuite with BeforeAndAfterAll {

  test("Country code only"){
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.countryCode = "GB"
    (new LocationProcessor).process("test", raw, processed)
    expectResult("United Kingdom") {
      processed.location.country
    }
  }

  test("State based sensitivity") {

    SpatialLayerDAO.addToCache(146.921099, -31.2532183, Map("cl22" -> "New South Wales"))
    SensitivityDAO.addToCache("Diuris disposita", "urn:lsid:biodiversity.org.au:apni.taxon:167966", true)

    val raw = new FullRecord
    val processed = new FullRecord
    raw.classification.scientificName = "Diuris disposita"
    processed.classification.scientificName = "Diuris disposita"
    processed.classification.taxonConceptID = "urn:lsid:biodiversity.org.au:apni.taxon:167966"
    processed.classification.taxonRankID = "7000"
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

  test("Not Sensitive") {
    val raw = new FullRecord
    val processed = new FullRecord
    processed.classification.setScientificName("Cataxia maculata")
    processed.classification.setTaxonConceptID("urn:lsid:biodiversity.org.au:afd.taxon:41cc4a69-06d4-4591-9afe-7af431b7153c")
    processed.classification.setTaxonRankID("7000")
    raw.location.decimalLatitude = "-27.56"
    raw.location.decimalLongitude = "152.28"
    raw.attribution.dataResourceUid = "dr359"
    (new LocationProcessor).process("test", raw, processed)
    expectResult("-27.56") {
      processed.location.decimalLatitude
    }
    expectResult("152.28") {
      processed.location.decimalLongitude
    }
    expectResult(true) {
      StringUtils.isBlank(processed.occurrence.dataGeneralizations)
    }
  }

  ignore("Already Generalised - Crex crex - Western Australia ! already generalised commented out of configuration") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.classification.setScientificName("Crex crex")
    processed.classification.setScientificName("Crex crex")
    processed.classification.setTaxonConceptID("urn:lsid:biodiversity.org.au:afd.taxon:2ef4ac9c-7dfb-4447-8431-e337355ac1ca")
    processed.classification.setTaxonRankID("7000")
    raw.location.decimalLatitude = "-31.9"
    raw.location.decimalLongitude = "116.5"
    raw.attribution.dataResourceUid = "dr359"
    (new LocationProcessor).process("test", raw, processed)
    expectResult("-31.9") {
      processed.location.decimalLatitude
    }
    expectResult("116.5") {
      processed.location.decimalLongitude
    }
    expectResult(true) {
      processed.occurrence.dataGeneralizations != null
    }
    expectResult(true){
      processed.occurrence.dataGeneralizations.contains("already generalised")
    }
  }

  test("Uncertainty in Precision") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.decimalLatitude = "-35.21667"
    raw.location.decimalLongitude = "144.81060"
    raw.location.coordinatePrecision = "100.66"
    val qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(true) {
     qas.find(_.code == AssertionCodes.UNCERTAINTY_IN_PRECISION.code) != None
    }
    expectResult("100") {
      processed.location.coordinateUncertaintyInMeters
    }
  }

  test("Uncertainty in meter") {
    val raw = new FullRecord
    var processed = new FullRecord
    raw.location.decimalLatitude = "-35.21667"
    raw.location.decimalLongitude = "144.81060"
    raw.location.coordinateUncertaintyInMeters = "100 meters"
    val qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(1) {
      qas.find(_.code == 27).get.qaStatus
    }
    expectResult("100.0") {
      processed.location.coordinateUncertaintyInMeters
    }
  }

  test("Coordinates Out Of Range") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.decimalLatitude = "91"
    raw.location.decimalLongitude = "121"
    raw.location.coordinateUncertaintyInMeters = "1000"
    var qas = (new LocationProcessor).process("test", raw, processed)

    expectResult(true) {
      qas.find(_.code == 5) != None
    }

    raw.location.decimalLatitude = "-32"
    raw.location.decimalLongitude = "190"
    qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(true) {
      qas.find(_.code == 5) != None
    }

    raw.location.decimalLatitude = "-32"
    raw.location.decimalLongitude = "120"
    qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(1) {
      qas.find(_.code == 5).get.qaStatus
    }

    raw.location.decimalLatitude = "-120"
    raw.location.decimalLongitude = "120"
    qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(true) {
      qas.find(_.code == 5) != None
    }

    raw.location.decimalLatitude = "-32"
    raw.location.decimalLongitude = "-200"
    qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(true) {
      qas.find(_.code == 5) != None
    }
  }

  test("Inverted Coordinates") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.decimalLatitude = "123.123"
    raw.location.decimalLongitude = "-34.29"
    val qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(true) {
      qas.find(_.code == 3) != None
    }
    expectResult("-34.29") {
      processed.location.decimalLatitude
    }
    expectResult("123.123") {
      processed.location.decimalLongitude
    }

  }

  test("Latitude Zero") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.decimalLatitude = "0"
    raw.location.decimalLongitude = "149.099"
    raw.location.coordinateUncertaintyInMeters = "100"
    raw.location.country = "Australia"
    val qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(true) {
      qas.size > 0
    }
    expectResult(true) {
      qas.map(q => q.code).contains(AssertionCodes.ZERO_LATITUDE_COORDINATES.code)
    }
  }

  test("Longitude Zero") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.decimalLatitude = "-34.222"
    raw.location.decimalLongitude = "0"
    raw.location.coordinateUncertaintyInMeters = "100"
    raw.location.country = "Australia"
    val qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(true) {
      qas.size > 0
    }
    expectResult(true) {
      qas.map(q => q.code).contains(AssertionCodes.ZERO_LONGITUDE_COORDINATES.code)
    }
  }

  test("Latitude Negated") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.decimalLatitude = "35.23"
    raw.location.decimalLongitude = "149.099"
    raw.location.coordinateUncertaintyInMeters = "100"
    raw.location.country = "Australia"
    val qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(true) {
      qas.size > 0
    }
    expectResult(true) {
      qas.map(q => q.code).contains(1)
    }
    expectResult("-35.23") {
      processed.location.decimalLatitude
    }
  }

  test("Longitude Negated") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.decimalLatitude = "-35.23"
    raw.location.decimalLongitude = "-149.099"
    raw.location.coordinateUncertaintyInMeters = "100"
    raw.location.country = "Australia"
    val qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(true) {
      qas.size > 0
    }
    expectResult(true) {
      qas.map(q => q.code).contains(2)
    }
    expectResult("149.099") {
      processed.location.decimalLongitude
    }
  }

  test("Habitat Mismatch - marine") {

    SpatialLayerDAO.addToCache(145.52, -40.857, Map("cl23" -> "Fake Marine Location"))

    val raw = new FullRecord
    val processed = new FullRecord
    val locationProcessor = new LocationProcessor
    processed.classification.taxonConceptID = "urn:lsid:biodiversity.org.au:afd.taxon:aa745ff0-c776-4d0e-851d-369ba0e6f537"
    raw.location.decimalLatitude = "-40.857" // this point is in the water
    raw.location.decimalLongitude = "145.52"
    raw.location.coordinateUncertaintyInMeters = "100"
    val qas = locationProcessor.process("test", raw, processed)
    expectResult(true) {
      val assertion = qas.find(_.code == AssertionCodes.COORDINATE_HABITAT_MISMATCH.code)
      val qaStatus = assertion match {
        case Some(qa) => qa.getQaStatus
        case None => false
      }
      qaStatus == 0
    }
  }

  test("Habitat Mismatch - terretrial") {

    SpatialLayerDAO.addToCache(133.85720, -23.73750, Map("cl24" -> "Fake Terrestrial Location"))
    val raw = new FullRecord
    val processed = new FullRecord
    val locationProcessor = new LocationProcessor
    processed.classification.taxonConceptID = "urn:lsid:biodiversity.org.au:afd.taxon:aa745ff0-c776-4d0e-851d-369ba0e6f537"

    raw.location.decimalLatitude = "-23.73750"  // this point is on land
    raw.location.decimalLongitude = "133.85720"
    val qas = locationProcessor.process("test", raw, processed)
    expectResult(true) {
      val assertion = qas.find(_.code == AssertionCodes.COORDINATE_HABITAT_MISMATCH.code)
      val qaStatus = assertion match {
        case Some(qa) => qa.getQaStatus
        case None => false
      }
      qaStatus == 1
    }
  }

  test("zero coordinates") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.decimalLatitude = "0.0"
    raw.location.decimalLongitude = "0.0"
    raw.location.coordinateUncertaintyInMeters = "100"
    val qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      qas.find(_.code == 4).get.qaStatus
    }
  }

  test("unknown country name") {
    val raw = new FullRecord
    var processed = new FullRecord
    raw.location.decimalLatitude = "-40.857"
    raw.location.decimalLongitude = "145.52"
    raw.location.coordinateUncertaintyInMeters = "100"
    raw.location.country = "dummy"
    val qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      qas.find(_.code == 6).get.qaStatus
    }
  }

  test("stateProvince coordinate mismatch") {

    SpatialLayerDAO.addToCache(146.921099, -31.2532183, Map("cl22" -> "New South Wales"))

    val raw = new FullRecord
    var processed = new FullRecord
    raw.location.decimalLatitude = "-31.2532183"
    raw.location.decimalLongitude = "146.921099"
    raw.location.coordinateUncertaintyInMeters = "100"
    raw.location.country = "Australia"
    raw.location.stateProvince = "Australian Capital Territory"
    val qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      qas.find(_.code == 18).get.qaStatus
    }
  }

  test("coordinates center of stateProvince") {

    SpatialLayerDAO.addToCache(146.921099, -31.2532183, Map("cl22" -> "New South Wales"))

    val raw = new FullRecord
    var processed = new FullRecord
    raw.location.decimalLatitude = "-31.2532183"
    raw.location.decimalLongitude = "146.921099"
    raw.location.coordinateUncertaintyInMeters = "100"
    raw.location.country = "Australia"
    raw.location.stateProvince = "New South Wales"
    val qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      qas.find(_.code == 22).get.qaStatus
    }
  }

  test("coordinates center of country") {

    SpatialLayerDAO.addToCache(167.95, -29.04, Map("cl21" -> "Norfolk Island"))

    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.country="Norfolk Island"
    raw.location.decimalLatitude="-29.04"
    raw.location.decimalLongitude="167.95"
    val qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      qas.find(_.code == 28).get.qaStatus
    }
  }

  test("country inferred from coordinates") {
    var raw = new FullRecord
    var processed = new FullRecord
    raw.location.decimalLatitude="-29.04"
    raw.location.decimalLongitude="167.95"
    var qas = (new LocationProcessor).process("test", raw, processed)

    expectResult(0){
      //qa test has failed
      qas.find(_.getName == "countryInferredByCoordinates").get.qaStatus
    }

    raw = new FullRecord
    processed = new FullRecord
    raw.location.decimalLatitude="-29.04"
    raw.location.decimalLongitude="167.95"
    raw.location.country="Norfolk island"
    qas = (new LocationProcessor).process("test", raw, processed)
    //qa test has passed
    expectResult(1)  {
      qas.find(_.getName == "countryInferredByCoordinates").get.qaStatus
    }
  }

  test ("country coordinate mismatch") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.country="Norfolk Island"
    raw.location.decimalLatitude = "-31.2532183"
    raw.location.decimalLongitude = "146.921099"
    var qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0){
      //qa test has failed
      qas.find(_.getName == "countryCoordinateMismatch").get.qaStatus
    }
    raw.location.decimalLatitude="-29.04"
    raw.location.decimalLongitude="167.95"
    qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(None){
      //no coordinate mismatch has occurred
      qas.find(_.getName == "countryCoordinateMismatch")
    }
  }

  test("uncertainty range mismatch") {
    val raw = new FullRecord
    var processed = new FullRecord
    raw.location.decimalLatitude = "-31.2532183"
    raw.location.decimalLongitude = "146.921099"
    raw.location.coordinateUncertaintyInMeters = "-1"
    val qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      qas.find(_.code == 24).get.qaStatus
    }
  }

  test("uncertainty not specified") {
    val raw = new FullRecord
    var processed = new FullRecord
    raw.location.decimalLatitude = "-31.2532183"
    raw.location.decimalLongitude = "146.921099"
    val qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      qas.find(_.code == 27).get.qaStatus
    }
  }

  test ("depth in feet") {
    val raw = new FullRecord
    var processed = new FullRecord
    raw.location.verbatimDepth = "100ft"
    val qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      qas.find(_.getName == "depthInFeet").get.qaStatus
    }
  }

  test ("altitude in feet") {
    val raw = new FullRecord
    var processed = new FullRecord
    raw.location.verbatimElevation = "100ft"
    val qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      qas.find(_.getName == "altitudeInFeet").get.qaStatus
    }
  }

  test("non numeric depth") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.verbatimDepth = "test"
    val qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      qas.find(_.code == 15).get.qaStatus
    }
  }

  test("non numeric altitude") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.verbatimElevation = "test"
    val qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      qas.find(_.code == 14).get.qaStatus
    }
  }

  test("depth out of range") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.verbatimDepth = "20000"
    var qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      qas.find(_.code == 11).get.qaStatus
    }
    raw.location.verbatimDepth = "200"
    qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(1) {
      qas.find(_.code == 11).get.qaStatus
    }
  }

  test("altitude out of range") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.verbatimElevation = "20000"
    var qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      qas.find(_.code == 7).get.qaStatus
    }
    raw.location.verbatimElevation = "-200"
    qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      qas.find(_.code == 7).get.qaStatus
    }
    raw.location.verbatimElevation = "100"
    qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(1) {
      qas.find(_.code == 7).get.qaStatus
    }
  }

  test("transposed min and max") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.minimumDepthInMeters = "20"
    raw.location.maximumDepthInMeters = "10"
    var qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      qas.find(_.code == 12).get.qaStatus
    }
    raw.location.maximumDepthInMeters = "100"
    raw.location.minimumElevationInMeters = "100"
    raw.location.maximumElevationInMeters = "20"
    qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      qas.find(_.code == 9).get.qaStatus
    }
    raw.location.maximumElevationInMeters = "test"
    qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(None) {
      qas.find(_.code == 9)
    }
  }

  test("Calculate lat/long from easting and northing") {
    val raw = new FullRecord
    val processed = new FullRecord
    processed.classification.setScientificName("Crex crex")
    processed.classification.setTaxonConceptID("urn:lsid:biodiversity.org.au:afd.taxon:2ef4ac9c-7dfb-4447-8431-e337355ac1ca")
    processed.classification.setTaxonRankID("7000")
    raw.location.easting = "539514.0"
    raw.location.northing = "5362674.0"
    raw.location.zone = "55"
    //No verbatim SRS supplied, GDA94 should be assumed
    raw.attribution.dataResourceUid = "dr359"
    raw.rowKey = "test"
    val assertions = (new LocationProcessor).process("test", raw, processed)
    expectResult("-41.88688") {
      processed.location.decimalLatitude
    }
    expectResult("147.47628") {
      processed.location.decimalLongitude
    }
    expectResult(0) {
      assertions.find(_.getName == "decimalLatLongCalculatedFromEastingNorthing").get.qaStatus
    }
    expectResult(false) {
      var coordinatesInverted = false
      for (qa <- assertions) {
        if (qa.getName == "invertedCoordinates" && qa.qaStatus == 0) {
          coordinatesInverted = true
        }
      }
      coordinatesInverted
    }
  }

  test("Calculate lat/long from verbatim lat/long supplied in degrees, minutes, seconds") {
    val raw = new FullRecord
    val processed = new FullRecord
    processed.classification.setScientificName("Crex crex")
    processed.classification.setTaxonConceptID("urn:lsid:biodiversity.org.au:afd.taxon:2ef4ac9c-7dfb-4447-8431-e337355ac1ca")
    processed.classification.setTaxonRankID("7000")
    raw.location.verbatimLatitude = "22° 2' 56\" N"
    raw.location.verbatimLongitude = "92° 25' 11\" E"
    raw.location.locationRemarks = "test remarks"
    raw.rowKey = "test"
    val assertions = (new LocationProcessor).process("test", raw, processed)
    expectResult("22.04889") {
      processed.location.decimalLatitude
    }
    expectResult("92.41972") {
      processed.location.decimalLongitude
    }
    //WGS 84 should be assumed
    expectResult("EPSG:4326") {
      processed.location.geodeticDatum
    }
    expectResult(0) {
      assertions.find(_.getName == "decimalLatLongCalculatedFromVerbatim").get.qaStatus
    }
  }

  test("Reproject decimal lat/long from AGD66 to WGS84") {
    val raw = new FullRecord
    val processed = new FullRecord
    processed.classification.setScientificName("Crex crex")
    processed.classification.setTaxonConceptID("urn:lsid:biodiversity.org.au:afd.taxon:2ef4ac9c-7dfb-4447-8431-e337355ac1ca")
    processed.classification.setTaxonRankID("7000")
    raw.location.decimalLatitude = "-35.126"
    raw.location.decimalLongitude = "150.681"
    raw.location.geodeticDatum = "EPSG:4202"
    raw.rowKey = "test"
    val assertions = (new LocationProcessor).process("test", raw, processed)
    expectResult("-35.125") {
      processed.location.decimalLatitude
    }
    expectResult("150.682") {
      processed.location.decimalLongitude
    }
    expectResult("EPSG:4326") {
      processed.location.geodeticDatum
    }
    expectResult(0) {
      assertions.find(_.getName == "decimalLatLongConverted").get.qaStatus
    }
    expectResult(false) {
      var coordinatesInverted = false
      for (qa <- assertions) {
        if (qa.getName == "invertedCoordinates" && qa.qaStatus == 0) {
          coordinatesInverted = true
        }
      }
      coordinatesInverted
    }
  }

  test("Calculate decimal latitude/longitude by reprojecting verbatim latitude/longitude") {
    val raw = new FullRecord
    val processed = new FullRecord
    processed.classification.setScientificName("Crex crex")
    processed.classification.setTaxonConceptID("urn:lsid:biodiversity.org.au:afd.taxon:2ef4ac9c-7dfb-4447-8431-e337355ac1ca")
    processed.classification.setTaxonRankID("7000")
    raw.location.verbatimLatitude = "-35.126"
    raw.location.verbatimLongitude = "150.681"
    raw.location.verbatimSRS = "EPSG:4202"
    raw.rowKey = "test"
    val assertions = (new LocationProcessor).process("test", raw, processed)
    expectResult("-35.125") {
      processed.location.decimalLatitude
    }
    expectResult("150.682") {
      processed.location.decimalLongitude
    }
    expectResult("EPSG:4326") {
      processed.location.geodeticDatum
    }
    expectResult(0) {
      assertions.find(_.getName == "decimalLatLongCalculatedFromVerbatim").get.qaStatus
    }
    expectResult(false) {
      var coordinatesInverted = false
      for (qa <- assertions) {
        if (qa.getName == "invertedCoordinates" && qa.qaStatus == 0 ) {
          coordinatesInverted = true
        }
      }
      coordinatesInverted
    }
  }

  test("Assume WGS84 when no CRS supplied") {
    val raw = new FullRecord
    val processed = new FullRecord
    processed.classification.setScientificName("Crex crex")
    processed.classification.setTaxonConceptID("urn:lsid:biodiversity.org.au:afd.taxon:2ef4ac9c-7dfb-4447-8431-e337355ac1ca")
    processed.classification.setTaxonRankID("7000")
    raw.location.decimalLatitude = "-34.9666709899902"
    raw.location.decimalLongitude = "138.733337402344"
    raw.rowKey = "test"
    val assertions = (new LocationProcessor).process("test", raw, processed)
    expectResult("-34.9666709899902") {
      processed.location.decimalLatitude
    }
    expectResult("138.733337402344") {
      processed.location.decimalLongitude
    }
    expectResult("EPSG:4326") {
      processed.location.geodeticDatum
    }
    expectResult(0) {
      assertions.find(_.getName == "geodeticDatumAssumedWgs84").get.qaStatus
    }
  }

  test("Convert verbatim lat/long in degrees then reproject to WGS84") {
    val raw = new FullRecord
    val processed = new FullRecord
    processed.classification.setScientificName("Crex crex")
    processed.classification.setTaxonConceptID("urn:lsid:biodiversity.org.au:afd.taxon:2ef4ac9c-7dfb-4447-8431-e337355ac1ca")
    processed.classification.setTaxonRankID("7000")
    raw.location.verbatimLatitude = "43°22'06\" S"
    raw.location.verbatimLongitude = "145°47'11\" E"
    raw.location.verbatimSRS = "EPSG:4202"
    raw.rowKey = "test"
    val assertions = (new LocationProcessor).process("test", raw, processed)
    expectResult("-43.36697") {
      processed.location.decimalLatitude
    }
    expectResult("145.78746") {
      processed.location.decimalLongitude
    }
    expectResult("EPSG:4326") {
      processed.location.geodeticDatum
    }
    expectResult(0) {
      assertions.find(_.getName == "decimalLatLongCalculatedFromVerbatim").get.qaStatus
    }
  }

  test("Test recognition of AGD66 as geodeticDatum") {
    val raw = new FullRecord
    var processed = new FullRecord
    processed.classification.setScientificName("Crex crex")
    processed.classification.setTaxonConceptID("urn:lsid:biodiversity.org.au:afd.taxon:2ef4ac9c-7dfb-4447-8431-e337355ac1ca")
    processed.classification.setTaxonRankID("7000")
    raw.location.decimalLatitude = "-35.126"
    raw.location.decimalLongitude = "150.681"
    raw.location.geodeticDatum = "AGD66"
    raw.rowKey = "test"
    val assertions = (new LocationProcessor).process("test", raw, processed)
    expectResult("-35.125") {
      processed.location.decimalLatitude
    }
    expectResult("150.682") {
      processed.location.decimalLongitude
    }
    expectResult("EPSG:4326") {
      processed.location.geodeticDatum
    }
    expectResult(0) {
      assertions.find(_.getName == "decimalLatLongConverted").get.qaStatus
    }
  }

  test("Test recognition of AGD66 as verbatimSRS") {
    val raw = new FullRecord
    var processed = new FullRecord
    processed.classification.setScientificName("Crex crex")
    processed.classification.setTaxonConceptID("urn:lsid:biodiversity.org.au:afd.taxon:2ef4ac9c-7dfb-4447-8431-e337355ac1ca")
    processed.classification.setTaxonRankID("7000")
    raw.location.verbatimLatitude = "-35.126"
    raw.location.verbatimLongitude = "150.681"
    raw.location.verbatimSRS = "AGD66"
    raw.rowKey = "test"
    val assertions = (new LocationProcessor).process("test", raw, processed)
    expectResult("-35.125") {
      processed.location.decimalLatitude
    }
    expectResult("150.682") {
      processed.location.decimalLongitude
    }
    expectResult("EPSG:4326") {
      processed.location.geodeticDatum
    }
    expectResult(0) {
      assertions.find(_.getName == "decimalLatLongCalculatedFromVerbatim").get.qaStatus
    }
  }

  test("Test bad geodeticDatum") {
    val raw = new FullRecord
    var processed = new FullRecord
    processed.classification.setScientificName("Crex crex")
    processed.classification.setTaxonConceptID("urn:lsid:biodiversity.org.au:afd.taxon:2ef4ac9c-7dfb-4447-8431-e337355ac1ca")
    processed.classification.setTaxonRankID("7000")
    raw.location.decimalLatitude = "-35.126"
    raw.location.decimalLongitude = "150.681"
    raw.location.geodeticDatum = "FOO"
    raw.rowKey = "test"
    val assertions = (new LocationProcessor).process("test", raw, processed)
    expectResult("-35.126") {
      processed.location.decimalLatitude
    }
    expectResult("150.681") {
      processed.location.decimalLongitude
    }
    expectResult(null) {
      processed.location.geodeticDatum
    }

    expectResult(0) {
      assertions.find(_.getName == "unrecognizedGeodeticDatum").get.qaStatus
    }
  }

  test("Test bad verbatimSRS") {
    val raw = new FullRecord
    var processed = new FullRecord
    processed.classification.setScientificName("Crex crex")
    processed.classification.setTaxonConceptID("urn:lsid:biodiversity.org.au:afd.taxon:2ef4ac9c-7dfb-4447-8431-e337355ac1ca")
    processed.classification.setTaxonRankID("7000")
    raw.location.verbatimLatitude = "-35.126"
    raw.location.verbatimLongitude = "150.681"
    raw.location.verbatimSRS = "FOO"
    raw.rowKey = "test"
    val assertions = (new LocationProcessor).process("test", raw, processed)
    expectResult(null) {
      processed.location.decimalLatitude
    }
    expectResult(null) {
      processed.location.decimalLongitude
    }
    expectResult(null) {
      processed.location.geodeticDatum
    }

    expectResult(0) {
      assertions.find(_.getName == "decimalLatLongCalculationFromVerbatimFailed").get.qaStatus
    }
  }

  test ("decimal coordinates not supplied") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.verbatimLatitude = "-35.126"
    raw.location.verbatimLongitude = "150.681"
    var qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      qas.find(_.getName == "decimalCoordinatesNotSupplied").get.qaStatus
    }
    raw.location.decimalLatitude = "-35.126"
    raw.location.decimalLongitude = "150.681"
    qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(1) {
      qas.find(_.getName == "decimalCoordinatesNotSupplied").get.qaStatus
    }
  }

  test ("precision range mismatch") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.verbatimLatitude = "-35.126"
    raw.location.verbatimLongitude = "150.681"
    raw.location.coordinatePrecision = "test"
    var qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      qas.find(_.getName == "precisionRangeMismatch").get.qaStatus
    }
    raw.location.coordinatePrecision = "700"
    qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(None) {
      //no error message because assumed to be coordinate uncertainty in metres
      qas.find(_.getName == "precisionRangeMismatch")
    }
    raw.location.coordinatePrecision = "0"
    qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      qas.find(_.getName == "precisionRangeMismatch").get.qaStatus
    }
    raw.location.coordinatePrecision = "0.01"
    qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(1) {
      qas.find(_.getName == "precisionRangeMismatch").get.qaStatus
    }
  }

  test ("coordinate precision mismatch") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.verbatimLatitude = "-35.126"
    raw.location.verbatimLongitude = "150.681"
    raw.location.coordinatePrecision = "0.001"
    var qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(1) {
      //no mismatch
      qas.find(_.getName == "coordinatePrecisionMismatch").get.qaStatus
    }
    raw.location.verbatimLongitude = "150.68"
    qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      //one mismatch
      qas.find(_.getName == "coordinatePrecisionMismatch").get.qaStatus
    }
    raw.location.verbatimLatitude = "-35.1"
    qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      //both mismatch
      qas.find(_.getName == "coordinatePrecisionMismatch").get.qaStatus
    }
  }

  test ("Missing georeference date") {
    val raw = new FullRecord
    val processed = new FullRecord

    var qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      //date is missing
      qas.find(_.getName == "missingGeoreferenceDate").get.qaStatus
    }

    raw.miscProperties.put("georeferencedDate", "2013-05-28")
    qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(1) {
      //date is in miscProperties
      qas.find(_.getName == "missingGeoreferenceDate").get.qaStatus
    }

    raw.location.georeferencedDate ="2013-05-28"
    raw.miscProperties.clear()
    qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(1) {
      //date is in field
      qas.find(_.getName == "missingGeoreferenceDate").get.qaStatus
    }
  }

  test ("location not supplied") {
    val raw = new FullRecord
    val processed = new FullRecord
    var qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(0) {
      //no location information
      qas.find(_.getName == "locationNotSupplied").get.qaStatus
    }
    raw.location.footprintWKT="my footprint"
    qas = (new LocationProcessor).process("test", raw, processed)
    expectResult(1) {
      //no location information
      qas.find(_.getName == "locationNotSupplied").get.qaStatus
    }
  }
}
