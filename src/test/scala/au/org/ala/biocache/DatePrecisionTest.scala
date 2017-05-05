package au.org.ala.biocache

import au.org.ala.biocache.model.{QualityAssertion, FullRecord}
import au.org.ala.biocache.processor.EventProcessor
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable.ArrayBuffer

/**
  * Tests for event date parsing. To run these tests create a new scala application
  * run configuration in your IDE.
  *
  * See http://www.scalatest.org/getting_started_with_fun_suite
  *
  * scala -cp scalatest-1.0.jar org.scalatest.tools.Runner -p . -o -s au.au.biocache.ProcessEventTests
  *
  * @author Dave Martin (David.Martin@csiro.au)
  */
@RunWith(classOf[JUnitRunner])
class DatePrecisionTest extends FunSuite {

  test("2000-01-01 with blank precision - date shouldn't be affected"){
    val e = new EventProcessor
    val r = new FullRecord
    val p = new FullRecord
    p.event.eventDate = "2000-01-01"
    r.event.eventDate = "2000-01-01"
    val assertions = new ArrayBuffer[QualityAssertion]
    e.checkPrecision(r, p, assertions)
    expectResult("2000-01-01"){ p.event.eventDate  }
  }

  test("2000-01-01 with month precision"){
    val e = new EventProcessor
    val r = new FullRecord
    val p = new FullRecord
    p.event.eventDate = "2000-01-01"
    r.event.eventDate = "2000-01-01"
    r.event.datePrecision = "O"
    val assertions = new ArrayBuffer[QualityAssertion]
    e.checkPrecision(r, p, assertions)
    expectResult("2000-01"){ p.event.eventDate  }
  }

  test("2000-01-01 with year precision"){
    val e = new EventProcessor
    val r = new FullRecord
    val p = new FullRecord
    p.event.eventDate = "2000-01-01"
    r.event.eventDate = "2000-01-01"
    r.event.datePrecision = "Y"
    val assertions = new ArrayBuffer[QualityAssertion]
    e.checkPrecision(r, p, assertions)
    expectResult("2000"){ p.event.eventDate  }
  }
}
