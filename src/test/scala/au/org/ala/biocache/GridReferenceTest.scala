package au.org.ala.biocache

import au.org.ala.biocache.model.QualityAssertion
import au.org.ala.biocache.processor.LocationProcessor
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class GridReferenceTest extends FunSuite {

  test("Convert OS grid reference to Northing / Easting") {

    val l = new LocationProcessor
    expectResult(Some((130000, 790000))) {
      l.osGridReferenceToEastingNorthing("NM39")
    }

    expectResult(Some((140000,799000))) {
      l.osGridReferenceToEastingNorthing("NM4099")
    }

    expectResult(Some((131600,800500))) {
      l.osGridReferenceToEastingNorthing("NG316005")
    }

    expectResult(Some((131000,791000))) {
      l.osGridReferenceToEastingNorthing("NM39A")
    }

    expectResult(Some((131000,799000))) {
      l.osGridReferenceToEastingNorthing("NM39E")
    }

    expectResult(Some((133000,793000))) {
      l.osGridReferenceToEastingNorthing("NM39G")
    }

    expectResult(Some((137000,795000))) {
      l.osGridReferenceToEastingNorthing("NM39S")
    }

    expectResult(Some((135000,797000))) {
      l.osGridReferenceToEastingNorthing("NM39N")
    }

    expectResult(Some((135000,799000))) {
      l.osGridReferenceToEastingNorthing("NM39P")
    }

    expectResult(Some((139000,799000))) {
      l.osGridReferenceToEastingNorthing("NM39Z")
    }
  }

  test("Convert OS grid reference to decimal latitude/longitude in WBS84") {
    val l = new LocationProcessor
    val assertions = new ArrayBuffer[QualityAssertion]
    val result = l.processGridReference("NM39", assertions)
    expectResult(false) { result.isEmpty }
    expectResult("56.92234") { result.get.latitude.toString }
    expectResult("-6.43865") { result.get.longitude.toString }
    expectResult("EPSG:4326") { result.get.datum.toString }
    expectResult("10000") { result.get.coordinateUncertaintyInMeters.toString }
  }
}
