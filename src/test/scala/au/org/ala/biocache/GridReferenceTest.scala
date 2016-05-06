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
    expectResult(Some((130000, 790000, Some(10000), 130000, 790000, 140000, 800000))) {
      l.osGridReferenceToEastingNorthing("NM39")
    }

    expectResult(Some((140000,799000, Some(1000), 140000, 799000, 141000, 800000))) {
      l.osGridReferenceToEastingNorthing("NM4099")
    }

    expectResult(Some((131600,800500, Some(100),131600,800500,131700,800600))) {
      l.osGridReferenceToEastingNorthing("NG316005")
    }

    expectResult(Some((130000,790000,Some(2000),130000,790000,132000,792000))) {
      l.osGridReferenceToEastingNorthing("NM39A")
    }

    expectResult(Some((130000,798000,Some(2000),130000,798000,132000,800000))) {
      l.osGridReferenceToEastingNorthing("NM39E")
    }

    expectResult(Some((132000,792000,Some(2000),132000,792000,134000,794000))) {
      l.osGridReferenceToEastingNorthing("NM39G")
    }

    expectResult(Some((136000,794000,Some(2000),136000,794000,138000,796000))) {
      l.osGridReferenceToEastingNorthing("NM39S")
    }

    expectResult(Some((134000,796000,Some(2000),134000,796000,136000,798000))) {
      l.osGridReferenceToEastingNorthing("NM39N")
    }

    expectResult(Some((134000,798000,Some(2000),134000,798000,136000,800000))) {
      l.osGridReferenceToEastingNorthing("NM39P")
    }

    expectResult(Some((138000,798000,Some(2000),138000,798000,140000,800000))) {
      l.osGridReferenceToEastingNorthing("NM39Z")
    }
  }

  test("Convert OS grid reference to decimal latitude/longitude in WBS84") {
    val l = new LocationProcessor
    val assertions = new ArrayBuffer[QualityAssertion]
    val result = l.processGridReference("NM39", assertions)
    expectResult(false) { result.isEmpty }
    expectResult("56.97001") { result.get.latitude.toString }
    expectResult("-6.36199") { result.get.longitude.toString }
    expectResult("EPSG:4326") { result.get.datum.toString }
    expectResult("10000") { result.get.coordinateUncertaintyInMeters.toString }
  }
}
