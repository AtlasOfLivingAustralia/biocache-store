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
  }

  test("Convert OS grid reference to decimal latitude/longitude in WBS84") {
    val l = new LocationProcessor
    val assertions = new ArrayBuffer[QualityAssertion]
    val result = l.processGridReference("NM39", assertions)
    expectResult(false) { result.isEmpty }
    expectResult("56.92234") { result.get._1.toString }
    expectResult("-6.43865") { result.get._2.toString }
    expectResult("EPSG:4326") { result.get._3.toString }
  }
}
