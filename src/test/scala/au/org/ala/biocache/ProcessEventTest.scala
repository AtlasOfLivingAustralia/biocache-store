package au.org.ala.biocache
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.apache.commons.lang.time.DateUtils
import java.util.Date
import java.text.SimpleDateFormat
import au.org.ala.biocache.processor.EventProcessor
import au.org.ala.biocache.model.FullRecord
import au.org.ala.biocache.util.DateUtil
import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * Tests for event date parsing. To run these tests create a new scala application
 * run configuration in your IDE.
 *
 * See http://www.scalatest.org/getting_started_with_fun_suite
 *
 * scala -cp scalatest-1.0.jar org.scalatest.tools.Runner -p . -o -s ay.au.biocache.ProcessEventTests
 *
 * @author Dave Martin (David.Martin@csiro.au)
 */
@RunWith(classOf[JUnitRunner])
class ProcessEventTest extends ConfigFunSuite {

  def QA_PASS: Integer = 1
  def QA_FAIL: Integer = 0
  
  test("00 month test"){
    val raw = new FullRecord("1234")
    raw.event.day ="0"
    raw.event.month = "0"
    raw.event.year = "0"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)
  }

  test("yyyy-dd-mm correctly sets year, month, day values in process object") {

    val raw = new FullRecord("1234")
    raw.event.eventDate = "1978-12-31"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12-31"){ processed.event.eventDate }
    expectResult("31"){ processed.event.day }
    expectResult("12"){ processed.event.month }
    expectResult("1978"){ processed.event.year }
    expectResult(null){ processed.event.eventDateEnd }
  }

  test("yyyy-dd-mmThh:MM:ss.SSS correctly sets year, month, day values in process object") {

    val raw = new FullRecord("1234")
    raw.event.eventDate = "2013-11-06T19:59:14.961"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("2013-11-06"){ processed.event.eventDate }
    expectResult("06"){ processed.event.day }
    expectResult("11"){ processed.event.month }
    expectResult("2013"){ processed.event.year }
    expectResult(null){ processed.event.eventDateEnd }
  }

  test("yyyy-dd-mmThh:MM+HH correctly sets year, month, day values in process object") {

    val raw = new FullRecord("1234")
    raw.event.eventDate = "2018-09-19T08:50+1000"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("2018-09-19"){ processed.event.eventDate }
    expectResult("19"){ processed.event.day }
    expectResult("09"){ processed.event.month }
    expectResult("2018"){ processed.event.year }
    expectResult(null){ processed.event.eventDateEnd }
  }

  test("yyyy-dd-mm verbatim date correctly sets year, month, day values in process object") {

    val raw = new FullRecord("1234")
    raw.event.verbatimEventDate = "1978-12-31/1978-12-31"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12-31"){ processed.event.eventDate }
    expectResult("31"){ processed.event.day }
    expectResult("12"){ processed.event.month }
    expectResult("1978"){ processed.event.year }

    //identical start and end dates cause eventDateEnd not set
    expectResult(null){ processed.event.eventDateEnd }
  }

  test("if year, day, month supplied, eventDate is correctly set") {

    val raw = new FullRecord("1234")
    raw.event.year = "1978"
    raw.event.month = "12"
    raw.event.day = "31"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12-31"){ processed.event.eventDate }
    expectResult("31"){ processed.event.day }
    expectResult("12"){ processed.event.month }
    expectResult("1978"){ processed.event.year }
  }

  test("if year supplied in 'yy' format, eventDate is correctly set") {

    val raw = new FullRecord("1234")
    raw.event.year = "78"
    raw.event.month = "12"
    raw.event.day = "31"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12-31"){ processed.event.eventDate }
    expectResult("31"){ processed.event.day }
    expectResult("12"){ processed.event.month }
    expectResult("1978"){ processed.event.year }
  }

  test("day month transposed") {

    val raw = new FullRecord("1234")
    raw.event.year = "78"
    raw.event.month = "16"
    raw.event.day = "6"
    val processed = raw.clone
    val qas = (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-06-16"){ processed.event.eventDate }
    expectResult("16"){ processed.event.day }
    expectResult("06"){ processed.event.month }
    expectResult("1978"){ processed.event.year }
    //expectResult(QA_PASS){ assertions.size }
    expectResult(QA_FAIL){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.DAY_MONTH_TRANSPOSED.code).get.getQaStatus}
  }

  test("invalid month test") {

    val raw = new FullRecord( "1234")
    val processed = new FullRecord("1234")
    raw.event.year = "78"
    raw.event.month = "16"
    raw.event.day = "16"

    val qas = (new EventProcessor).process("1234", raw, processed)

    expectResult(null){ processed.event.eventDate }
    expectResult("16"){ processed.event.day }
    expectResult(null){ processed.event.month }
    expectResult("1978"){ processed.event.year }

    //expectResult(QA_PASS){ assertions.size }
    expectResult(QA_FAIL){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
  }

  test("invalid month test > 12") {

    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    raw.event.year = "1978"
    raw.event.month = "40"
    raw.event.day = "16"

    val qas = (new EventProcessor).process("1234", raw, processed)

    expectResult(null){ processed.event.eventDate }
    expectResult("16"){ processed.event.day }
    expectResult(null){ processed.event.month }
    expectResult("1978"){ processed.event.year }

    //expectResult(QA_PASS){ assertions.size }
    expectResult(QA_FAIL){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
  }

  test("year = 11, month = 02, day = 01") {

    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    raw.event.year = "11"
    raw.event.month = "02"
    raw.event.day = "01"

    val qas = (new EventProcessor).process("1234", raw, processed)

    expectResult("2011-02-01"){ processed.event.eventDate }
    expectResult("1"){ processed.event.day }
    expectResult("02"){ processed.event.month }
    expectResult("2011"){ processed.event.year }

    //expectResult(0){ assertions.size }
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
  }

  test("1973-10-14") {

    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    raw.event.eventDate = "1973-10-14"

    val qas = (new EventProcessor).process("1234", raw, processed)

    expectResult("1973-10-14"){ processed.event.eventDate }
    expectResult("14"){ processed.event.day }
    expectResult("10"){ processed.event.month }
    expectResult("1973"){ processed.event.year }

    //expectResult(0){ assertions.size }
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
  }

  test("today"){
    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    val sf = new SimpleDateFormat("yyyy-MM-dd")
    raw.event.eventDate = sf.format(new Date())
    val qas = (new EventProcessor).process("1234", raw, processed)
    expectResult(DateUtil.getCurrentYear.toString){ processed.event.year }
    //expectResult(0){ assertions.size }
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
  }

  test("future date"){
    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    // Add two days on, as one day in the future is not flagged to allow for observations from other parts of the world
    raw.event.eventDate = LocalDate.now().plusDays(2).format(DateTimeFormatter.ISO_LOCAL_DATE)
    val qas = (new EventProcessor).process("1234", raw, processed)
    expectResult(DateUtil.getCurrentYear.toString){ processed.event.year }
    expectResult(true){ qas.size > 0 }
    expectResult(QA_FAIL){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
  }

  test ("Identification predates the occurrence") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.identification.dateIdentified = "2012-01-01"
    raw.event.eventDate = " 2013-01-01"

    var qas = (new EventProcessor).process("test", raw, processed)
    expectResult(QA_FAIL) {
      //the identification happened before the collection !!
      qas.find {_.getName == "idPreOccurrence"}.get.qaStatus
    }

    raw.identification.dateIdentified = "2013-01-01"
    qas = (new EventProcessor).process("test", raw, processed)
    expectResult(QA_PASS) {
      //the identification happened at the same time of the collection
      qas.find {_.getName == "idPreOccurrence"}.get.qaStatus
    }
  }

  test ("Georeferencing postdates the occurrence: fail") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.georeferencedDate = "2013-04-01"
    raw.event.eventDate = " 2013-01-01"

    val qas = (new EventProcessor).process("test", raw, processed)
    expectResult(QA_FAIL) {
      //the georeferencing happened after the collection !!
      qas.find {_.getName == "georefPostDate"}.get.qaStatus
    }
  }

  test ("Georeferencing postdates the occurrence: pass") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.location.georeferencedDate = "2013-01-01"
    raw.event.eventDate = " 2013-01-01"
    val qas = (new EventProcessor).process("test", raw, processed)
    expectResult(QA_PASS) {
      //the georeferecing happened at the same time as the collection
      qas.find {_.getName == "georefPostDate"}.get.qaStatus
    }
  }

  test("First of month/year/century") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.day ="1"
    raw.event.month="1"
    raw.event.year="2000"

    val qas = (new EventProcessor).process("test", raw, processed)
    expectResult(QA_FAIL) {
      //date is first of month
      qas.find {_.getName == "firstOfMonth"}.get.qaStatus
    }
    expectResult(QA_FAIL) {
      //date is also the first of the year
      qas.find {_.getName == "firstOfYear"}.get.qaStatus
    }
    expectResult(QA_FAIL) {
      //date is also the first of the century
      qas.find {_.getName == "firstOfCentury"}.get.qaStatus
    }
  }

  test("First of month/year") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.day ="1"
    raw.event.month="1"
    raw.event.year="2001"

    val qas = (new EventProcessor).process("test", raw, processed)
    expectResult(QA_FAIL) {
      //date is first of month
      qas.find {_.getName == "firstOfMonth"}.get.qaStatus
    }
    expectResult(QA_FAIL) {
      //date is also the first of the year
      qas.find {_.getName == "firstOfYear"}.get.qaStatus
    }
    expectResult(QA_PASS) {
      //date is NOT the first of the century
      qas.find {_.getName == "firstOfCentury"}.get.qaStatus
    }
  }

  test("First of month") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.day ="1"
    raw.event.month="2"
    raw.event.year="2001"

    val qas = (new EventProcessor).process("test", raw, processed)
    expectResult(QA_FAIL) {
      //date is first of month
      qas.find {_.getName == "firstOfMonth"}.get.qaStatus
    }
    expectResult(QA_PASS) {
      //date is NOT the first of the year
      qas.find {_.getName == "firstOfYear"}.get.qaStatus
    }
    expectResult(None) {
      //date is NOT the first of the century  - not tested since the month is not January
      qas.find {_.getName == "firstOfCentury"}
    }
  }

  test("Not first of test") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.day = "2"
    raw.event.month="2"
    raw.event.year="2001"

    val qas = (new EventProcessor).process("test", raw, processed)
    expectResult(QA_PASS) {
      //date is NOT first of month
      qas.find {_.getName == "firstOfMonth"}.get.qaStatus
    }
    expectResult(None) {
      //date is NOT the first of the year - gtested since the day is not 1
      qas.find {_.getName == "firstOfYear"}
    }
    expectResult(None) {
      //date is NOT the first of the century - not tested since the month is not January
      qas.find {_.getName == "firstOfCentury"}
    }
  }

  test("Year only - results in incomplete date error but NOT invalid date"){
    val raw = new FullRecord
    val processed = new FullRecord

    raw.event.eventDate="1978"

    val qas = (new EventProcessor).process("test",raw,processed)
    //AssertionCodes.INVALID_COLLECTION_DATE
    println(qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code))
    println(qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INCOMPLETE_COLLECTION_DATE.code))
  }

  test("valid but incomplete event year") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.year="2014"
    val qas = (new EventProcessor).process("test",raw,processed)
    expectResult(QA_FAIL){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INCOMPLETE_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
  }

  test("valid and complete day, month and year") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.year="2014"
    raw.event.month="01"
    raw.event.day="11"
    val qas = (new EventProcessor).process("test",raw,processed)
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INCOMPLETE_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
  }

  test("valid but incomplete eventDate") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.eventDate="2014-02"
    val qas = (new EventProcessor).process("test",raw,processed)
    expectResult(QA_FAIL){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INCOMPLETE_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
  }

  test("invalid date - year month") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.eventDate="2012-22"
    val qas = (new EventProcessor).process("test",raw,processed)
    expectResult(QA_FAIL){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
  }
  
  test("dateIdentified as a year before eventDate") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.eventDate="2012-02-01"
    raw.identification.dateIdentified="2011"
    val qas = (new EventProcessor).process("test",raw,processed)
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(QA_FAIL){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.ID_PRE_OCCURRENCE.code).get.getQaStatus}
  }

  test("dateIdentified as a year the same as eventDate first of year") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.eventDate="2012-01-01"
    raw.identification.dateIdentified="2012"
    val qas = (new EventProcessor).process("test",raw,processed)
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.ID_PRE_OCCURRENCE.code).get.getQaStatus}
  }

  test("dateIdentified before eventDate") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.eventDate="2012-02-01"
    raw.identification.dateIdentified="2012-01-01"
    val qas = (new EventProcessor).process("test",raw,processed)
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(QA_FAIL){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.ID_PRE_OCCURRENCE.code).get.getQaStatus}
  }

  test("dateIdentified the same as eventDate") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.eventDate="2012-02-01"
    raw.identification.dateIdentified="2012-02-01"
    val qas = (new EventProcessor).process("test",raw,processed)
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.ID_PRE_OCCURRENCE.code).get.getQaStatus}
  }
  
  test("dateIdentified after eventDate") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.eventDate="2012-02-01"
    raw.identification.dateIdentified="2012-03-01"
    val qas = (new EventProcessor).process("test",raw,processed)
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.ID_PRE_OCCURRENCE.code).get.getQaStatus}
  }
  
  test("georeferencedDate before eventDate") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.eventDate="2012-02-01"
    raw.location.georeferencedDate="2012-01-01"
    val qas = (new EventProcessor).process("test",raw,processed)
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(QA_FAIL){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.GEOREFERENCE_POST_OCCURRENCE.code).get.getQaStatus}
  }

  test("georeferencedDate the same as eventDate") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.eventDate="2012-02-01"
    raw.location.georeferencedDate="2012-02-01"
    val qas = (new EventProcessor).process("test",raw,processed)
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.GEOREFERENCE_POST_OCCURRENCE.code).get.getQaStatus}
  }
  
  test("georeferencedDate after eventDate") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.eventDate="2012-02-01"
    raw.location.georeferencedDate="2012-03-01"
    val qas = (new EventProcessor).process("test",raw,processed)
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(QA_FAIL){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.GEOREFERENCE_POST_OCCURRENCE.code).get.getQaStatus}
  }
  
  test("valid and complete event date") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.eventDate="2014-02-15"
    val qas = (new EventProcessor).process("test",raw,processed)
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INCOMPLETE_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
  }

  test("valid and incomplete event date") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.eventDate="2014-02"
    val qas = (new EventProcessor).process("test",raw,processed)
    expectResult(QA_FAIL){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INCOMPLETE_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
  }

  test("valid and incomplete verbatim event date") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.verbatimEventDate="2014-02"
    val qas = (new EventProcessor).process("test",raw,processed)
    expectResult(QA_FAIL){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INCOMPLETE_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
  }

  test("valid and complete verbatim event date") {
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.verbatimEventDate="2014-02-15"
    val qas = (new EventProcessor).process("test",raw,processed)
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INCOMPLETE_COLLECTION_DATE.code).get.getQaStatus}
    expectResult(QA_PASS){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getQaStatus}
  }

  test("First Fleet Tests"){
    var raw = new FullRecord
    val processed = new FullRecord
    raw.event.year="1788"
    raw.event.month="01"
    raw.event.day="26"
    var qas = (new EventProcessor).process("test", raw, processed)
    expectResult("First Fleet arrival implies a null date"){qas.find(_.code ==au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getComment}

    raw = new FullRecord
    raw.event.eventDate = "1788-01-26"
    qas = (new EventProcessor).process("test", raw, processed)
    expectResult("First Fleet arrival implies a null date"){qas.find(_.code == au.org.ala.biocache.vocab.AssertionCodes.INVALID_COLLECTION_DATE.code).get.getComment}
  }

  test("00 month and day"){
    val raw = new FullRecord
    val processed = new FullRecord
    raw.event.eventDate="2014-00-00"
    //should be an invalid date
  }

  test("if year, day, month, eventDate supplied, eventDate is used for eventDateEnd") {

    val raw = new FullRecord( "1234")
    raw.event.eventDate = "1978-12-31/1979-01-02"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12-31"){ processed.event.eventDate }
    expectResult(null){ processed.event.day }
    expectResult(null){ processed.event.month }
    expectResult(null){ processed.event.year }
    expectResult("1979-01-02"){ processed.event.eventDateEnd }
  }

  test("if year, day, month, verbatimEventDate supplied, verbatimEventDate is used for eventDateEnd") {

    val raw = new FullRecord("1234")
    raw.event.year = "1978"
    raw.event.month = "12"
    raw.event.day = "31"
    raw.event.verbatimEventDate = "1978-12-31/1979-01-02"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12-31"){ processed.event.eventDate }
    expectResult(null){ processed.event.day }
    expectResult(null){ processed.event.month }
    expectResult(null){ processed.event.year }
    expectResult("1979-01-02"){ processed.event.eventDateEnd }
  }

  test("if eventDate supplied, eventDate is used for eventDateEnd") {

    val raw = new FullRecord("1234")
    raw.event.eventDate = "1978-12-31/1979-01-02"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12-31"){ processed.event.eventDate }
    expectResult("1979-01-02"){ processed.event.eventDateEnd }
  }

  test("if verbatimEventDate supplied, verbatimEventDate is used for eventDateEnd") {

    val raw = new FullRecord("1234")
    raw.event.verbatimEventDate = "1978-12-31/1979-01-02"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12-31"){ processed.event.eventDate }
    expectResult("1979-01-02"){ processed.event.eventDateEnd }
  }

  test("separate start end end dates") {

    val raw = new FullRecord("1234")
    raw.event.eventDate = "31/12/1978"
    raw.event.eventDateEnd = "02/01/1979"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12-31"){ processed.event.eventDate }
    expectResult("1979-01-02"){ processed.event.eventDateEnd }
  }

  test("separate start end dates - month precision") {

    val raw = new FullRecord("1234")
    raw.event.eventDate = "01/12/1978"
    raw.event.eventDateEnd = "31/12/1978"
    raw.event.datePrecision = "M"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12"){ processed.event.eventDate }
    expectResult("1978-12"){ processed.event.eventDateEnd }
    expectResult("Month"){ processed.event.datePrecision }
  }

  test("separate start end dates - day precision") {

    val raw = new FullRecord("1234")
    raw.event.eventDate = "01/12/1978"
    raw.event.eventDateEnd = "01/12/1978"
    raw.event.datePrecision = "D"
    val processed = raw.clone
    (new EventProcessor).process("1234", raw, processed)

    expectResult("1978-12-01"){ processed.event.eventDate }
    expectResult("1978-12-01"){ processed.event.eventDateEnd }
    expectResult("Day"){ processed.event.datePrecision }
    expectResult("01"){ processed.event.day }
    expectResult("12"){ processed.event.month }
    expectResult("1978"){ processed.event.year }
  }

  test("separate start end dates - day precision 2") {

    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    raw.event.eventDate = "04/08/2009"
    raw.event.eventDateEnd = "04/08/2009"
    raw.event.datePrecision = "Day"

    (new EventProcessor).process("1234", raw, processed)

    expectResult("2009-08-04"){ processed.event.eventDate }
    expectResult("2009-08-04"){ processed.event.eventDateEnd }
    expectResult("Day"){ processed.event.datePrecision }
    expectResult("04"){ processed.event.day }
    expectResult("08"){ processed.event.month }
    expectResult("2009"){ processed.event.year }
  }

  test("separate start end dates - day precision 2 - year range") {

    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    raw.event.eventDate = "01/01/2005"
    raw.event.eventDateEnd = "31/12/2009"
    raw.event.datePrecision = "YY"

    (new EventProcessor).process("1234", raw, processed)

    expectResult("2005"){ processed.event.eventDate }
    expectResult("2009"){ processed.event.eventDateEnd }
    expectResult("Year Range"){ processed.event.datePrecision }
    expectResult(null){ processed.event.day }
    expectResult(null){ processed.event.month }
    expectResult(null){ processed.event.year }
  }

    test("invalid date" ) {

      val raw = new FullRecord("1234")
      val processed = new FullRecord("1234")
      raw.event.eventDate = "26-6-5"
      raw.event.eventDateEnd = null
      raw.event.datePrecision = null

      (new EventProcessor).process("1234", raw, processed)

      expectResult(null){ processed.event.eventDate }
      expectResult(null){ processed.event.eventDateEnd }
    }

  test("ambiguous date 26-6-5" ) {

    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    raw.event.eventDate = "26-6-5"
    raw.event.eventDateEnd = null
    raw.event.datePrecision = null

    (new EventProcessor).process("1234", raw, processed)

    expectResult(null){ processed.event.eventDate }
    expectResult(null){ processed.event.eventDateEnd }
  }

  test("ambiguous date 24-6-2" ) {

    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    raw.event.eventDate = "24-6-2"
    raw.event.eventDateEnd = null
    raw.event.datePrecision = null

    (new EventProcessor).process("1234", raw, processed)

    expectResult(null){ processed.event.eventDate }
    expectResult(null){ processed.event.eventDateEnd }
  }

  test("24-5-26 unparseable" ) {

    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    raw.event.eventDate = "24-5-26"
    raw.event.verbatimEventDate = "24-5-26"
    raw.event.eventDateEnd = null
    raw.event.datePrecision = null

    (new EventProcessor).process("1234", raw, processed)

    expectResult(null){ processed.event.eventDate }
    expectResult(null){ processed.event.eventDateEnd }
  }

  test("2002-02-02 eventDate, 02/02/2 verbatim" ) {

    val raw = new FullRecord("1234")
    val processed = new FullRecord("1234")
    raw.event.eventDate = null
    raw.event.verbatimEventDate = "02/02/2"
    raw.event.eventDateEnd = null
    raw.event.datePrecision = null

    (new EventProcessor).process("1234", raw, processed)

    expectResult(null){ processed.event.eventDate }
    expectResult(null){ processed.event.eventDateEnd }
  }

}