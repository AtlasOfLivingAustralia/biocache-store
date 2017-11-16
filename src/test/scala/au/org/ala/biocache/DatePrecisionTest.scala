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

  test("2000-01-01 with month precision - 'Month'"){
    val e = new EventProcessor
    val r = new FullRecord
    val p = new FullRecord
    p.event.eventDate = "2000-01-01"
    r.event.eventDate = "2000-01-01"
    r.event.datePrecision = "Month"
    val assertions = new ArrayBuffer[QualityAssertion]
    e.checkPrecision(r, p, assertions)
    expectResult("2000-01"){ p.event.eventDate  }
  }

  test("2000-01-01 with month precision - 'M'"){
    val e = new EventProcessor
    val r = new FullRecord
    val p = new FullRecord
    p.event.eventDate = "2000-01-01"
    r.event.eventDate = "2000-01-01"
    r.event.datePrecision = "M"
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

  test("2000-01-01 with year precision - 'year'"){
    val e = new EventProcessor
    val r = new FullRecord
    val p = new FullRecord
    p.event.eventDate = "2000-01-01"
    r.event.eventDate = "2000-01-01"
    r.event.datePrecision = "YEAR"
    val assertions = new ArrayBuffer[QualityAssertion]
    e.checkPrecision(r, p, assertions)
    expectResult("2000"){ p.event.eventDate  }
  }

  test("2000-01-01 2003-01-01 with year range'"){
    val e = new EventProcessor
    val r = new FullRecord
    val p = new FullRecord
    p.event.eventDate = "2000-01-01"
    p.event.eventDate = "2003-01-01"
    r.event.datePrecision = "YY"
    val assertions = new ArrayBuffer[QualityAssertion]
    e.checkPrecision(r, p, assertions)
    expectResult(null){ p.event.year  }
    expectResult(null){ p.event.month  }
    expectResult(null){ p.event.day  }
    expectResult("Year Range"){ p.event.datePrecision  }
  }

  test("2010-01-01 31/12/2010 with day range provided as separate dates'"){
    val e = new EventProcessor
    val r = new FullRecord
    val p = new FullRecord
    p.event.eventDate = "2010-01-01"
    p.event.eventDateEnd = "2010-12-31"
    p.event.day = "01"
    p.event.month = "12"
    p.event.year = "2010"
    val assertions = new ArrayBuffer[QualityAssertion]
    e.checkPrecision(r, p, assertions)
    expectResult("2010"){ p.event.year  }
    expectResult(null){ p.event.month  }
    expectResult(null){ p.event.day  }
    expectResult("Year"){ p.event.datePrecision  }
  }

  test("year range - without a precision supplied"){
    val e = new EventProcessor
    val r = new FullRecord
    val p = new FullRecord
    p.event.eventDate = "2000"
    p.event.eventDateEnd = "2001"
    val assertions = new ArrayBuffer[QualityAssertion]
    e.checkPrecision(r, p, assertions)
    expectResult(null){ p.event.year  }
    expectResult("Year Range"){ p.event.datePrecision  }
  }

  test("month range - without a precision supplied"){
    val e = new EventProcessor
    val r = new FullRecord
    val p = new FullRecord
    p.event.eventDate = "2000-01"
    p.event.eventDateEnd = "2001-02"
    val assertions = new ArrayBuffer[QualityAssertion]
    e.checkPrecision(r, p, assertions)
    expectResult(null){ p.event.year  }
    expectResult("Month Range"){ p.event.datePrecision  }
    expectResult(null){ p.event.year  }
    expectResult(null){ p.event.month  }
    expectResult(null){ p.event.day  }
  }

  test("day precision - without a precision supplied"){
    val e = new EventProcessor
    val r = new FullRecord
    val p = new FullRecord
    p.event.eventDate = "2000-01-01"
    p.event.eventDateEnd = ""
    p.event.day = "01"
    p.event.month = "01"
    p.event.year = "2000"
    val assertions = new ArrayBuffer[QualityAssertion]
    e.checkPrecision(r, p, assertions)
    expectResult("Day"){ p.event.datePrecision  }
    expectResult("2000"){ p.event.year  }
    expectResult("01"){ p.event.month  }
    expectResult("01"){ p.event.day  }
  }

  test("day precision (with end date) - without a precision supplied"){
    val e = new EventProcessor
    val r = new FullRecord
    val p = new FullRecord
    p.event.eventDate = "2000-01-01"
    p.event.eventDateEnd = "2000-01-01"
    p.event.day = "01"
    p.event.month = "01"
    p.event.year = "2000"
    val assertions = new ArrayBuffer[QualityAssertion]
    e.checkPrecision(r, p, assertions)
    expectResult("Day"){ p.event.datePrecision  }
    expectResult("2000"){ p.event.year  }
    expectResult("01"){ p.event.month  }
    expectResult("01"){ p.event.day  }
  }

  test("month precision - without a precision supplied"){
    val e = new EventProcessor
    val r = new FullRecord
    val p = new FullRecord
    p.event.eventDate = "2000-01"
    p.event.eventDateEnd = ""
    p.event.month = "01"
    p.event.year = "2000"
    val assertions = new ArrayBuffer[QualityAssertion]
    e.checkPrecision(r, p, assertions)
    expectResult("Month"){ p.event.datePrecision  }
    expectResult("2000"){ p.event.year  }
    expectResult("01"){ p.event.month  }
    expectResult(null){ p.event.day  }
  }

  test("month precision (with end date) - without a precision supplied"){
    val e = new EventProcessor
    val r = new FullRecord
    val p = new FullRecord
    p.event.eventDate = "2000-01"
    p.event.eventDateEnd = "2000-01"
    p.event.month = "01"
    p.event.year = "2000"
    val assertions = new ArrayBuffer[QualityAssertion]
    e.checkPrecision(r, p, assertions)
    expectResult("Month"){ p.event.datePrecision  }
    expectResult("2000"){ p.event.year  }
    expectResult("01"){ p.event.month  }
    expectResult(null){ p.event.day  }
  }

  test("year precision - without a precision supplied"){
    val e = new EventProcessor
    val r = new FullRecord
    val p = new FullRecord
    p.event.eventDate = "2000"
    p.event.eventDateEnd = ""
    p.event.year = "2000"
    val assertions = new ArrayBuffer[QualityAssertion]
    e.checkPrecision(r, p, assertions)
    expectResult("Year"){ p.event.datePrecision  }
    expectResult("2000"){ p.event.year  }
    expectResult(null){ p.event.month  }
    expectResult(null){ p.event.day  }
  }

  test("year precision (with end date) - without a precision supplied"){
    val e = new EventProcessor
    val r = new FullRecord
    val p = new FullRecord
    p.event.eventDate = "2000"
    p.event.eventDateEnd = "2000"
    p.event.year = "2000"
    val assertions = new ArrayBuffer[QualityAssertion]
    e.checkPrecision(r, p, assertions)
    expectResult("Year"){ p.event.datePrecision  }
    expectResult("2000"){ p.event.year  }
    expectResult(null){ p.event.month  }
    expectResult(null){ p.event.day  }
  }
}
