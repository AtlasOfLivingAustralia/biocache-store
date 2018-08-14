package au.org.ala.biocache
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.Assertions._
import au.org.ala.biocache.parser.DateParser

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
class DateParserTest extends FunSuite {

  test("Single Year"){
    val result = DateParser.parseDate("1978")
    expectResult(false){ result.isEmpty }
    expectResult("1978"){ result.get.startYear }
    expectResult("1978"){ result.get.endYear }
    expectResult(false){ result.get.singleDate }
  }

  test("Single Year with full month"){
    val result = DateParser.parseDate("1978-April-01")
    expectResult(false){ result.isEmpty }
    expectResult("1978"){ result.get.startYear }
    expectResult("1978"){ result.get.endYear }
    expectResult("04"){ result.get.startMonth }
    expectResult("01"){ result.get.startDay }
    expectResult(true){ result.get.singleDate }
  }

  test("Single Year with full month - long name"){
    val result = DateParser.parseDate("1978-December-01")
    expectResult(false){ result.isEmpty }
    expectResult("1978"){ result.get.startYear }
    expectResult("1978"){ result.get.endYear }
    expectResult("12"){ result.get.startMonth }
    expectResult("01"){ result.get.startDay }
    expectResult(true){ result.get.singleDate }
  }

  test("Single date") {

    val result = DateParser.parseDate("1978-01-01")

    expectResult(false){ result.isEmpty }
    expectResult("1978"){ result.get.startYear }
    expectResult("1978"){ result.get.endYear }
    expectResult(true){ result.get.singleDate }
  }

  test("Single date with time") {

    val result = DateParser.parseDate("2009-02-20T08:40Z")

    expectResult(false){ result.isEmpty }
    expectResult("2009"){ result.get.startYear }
    expectResult("02"){ result.get.startMonth }
    expectResult("20"){ result.get.startDay }
    expectResult(true){ result.get.singleDate }
  }

  test("Single date with time and seconds") {

    val result = DateParser.parseDate("2009-02-20T08:40:01Z")

    expectResult(false){ result.isEmpty }
    expectResult("2009"){ result.get.startYear }
    expectResult("02"){ result.get.startMonth }
    expectResult("20"){ result.get.startDay }
    expectResult(true){ result.get.singleDate }
  }

  test("Single month date") {

    val result = DateParser.parseDate("1978-01")

    expectResult(false){ result.isEmpty }
    expectResult("1978"){ result.get.startYear }
    expectResult("1978"){ result.get.endYear }
    expectResult("01"){ result.get.startMonth }
    expectResult("01"){ result.get.endMonth }
    expectResult(true){ result.get.singleDate }
  }

  test("Single month date with trailing hyphen") {

    val result = DateParser.parseDate("1978-01-")

    expectResult(false){ result.isEmpty }
    expectResult("1978"){ result.get.startYear }
    expectResult("1978"){ result.get.endYear }
    expectResult("01"){ result.get.startMonth }
    expectResult("01"){ result.get.endMonth }
    expectResult(true){ result.get.singleDate }
  }

  test("Date range") {

    val result = DateParser.parseDate("1978-01-01/1979-02-13")

    expectResult(false){ result.isEmpty }
    expectResult("1978"){ result.get.startYear }
    expectResult("1979"){ result.get.endYear }
    expectResult("01"){ result.get.startMonth }
    expectResult("02"){ result.get.endMonth }
    expectResult(false){ result.get.singleDate }
  }

  test("Month range") {

    val result = DateParser.parseDate("1978-01/1979-05")

    expectResult(false){ result.isEmpty }
    expectResult("1978"){ result.get.startYear }
    expectResult("1979"){ result.get.endYear }
    expectResult("01"){ result.get.startMonth }
    expectResult("05"){ result.get.endMonth }
    expectResult(false){ result.get.singleDate }
  }

  test("Month range trailing hyphens") {

    val result = DateParser.parseDate("1978-01-/1979-05-")

    expectResult(false){ result.isEmpty }
    expectResult("1978"){ result.get.startYear }
    expectResult("1979"){ result.get.endYear }
    expectResult("01"){ result.get.startMonth }
    expectResult("05"){ result.get.endMonth }
    expectResult(false){ result.get.singleDate }
  }

  test("Year range") {

    val result = DateParser.parseDate("1978/1991")

    expectResult(false){ result.isEmpty }
    expectResult("1978"){ result.get.startYear }
    expectResult("1991"){ result.get.endYear }
    expectResult(false){ result.get.singleDate }
  }

  test("Same century range") {

    val result = DateParser.parseDate("1978/91")

    expectResult(false){ result.isEmpty }
    expectResult("1978"){ result.get.startYear }
    expectResult("1991"){ result.get.endYear }
    expectResult(false){ result.get.singleDate }
  }

  test("2002-03-10 00:00:00.0/2002-03-10 00:00:00.0") {

    val result = DateParser.parseDate("2002-03-10 00:00:00.0/2003-03-10 00:00:00.0")

    expectResult(false){ result.isEmpty }
    expectResult("2002"){ result.get.startYear }
    expectResult("2003"){ result.get.endYear }
    expectResult(false){ result.get.singleDate }
  }

  test("2005-06-12 00:00:00.0/2005-06-12 00:00:00.0") {
    val result = DateParser.parseDate("2005-06-12 00:00:00.0/2005-06-12 00:00:00.0")
    expectResult(false){ result.isEmpty }
    expectResult("2005"){ result.get.startYear }
    expectResult("2005"){ result.get.endYear }
    expectResult(true){ result.get.singleDate }
  }

  test("12-06-2005") {
    val result = DateParser.parseDate("12-06-2005")
    expectResult(false){ result.isEmpty }
    expectResult("2005"){ result.get.startYear }
    expectResult("2005"){ result.get.endYear }
    expectResult(true){ result.get.singleDate }
  }

  test("Mon Apr 23 00:00:00 EST 1984/Sun Apr 29 00:00:00 EST 1984") {
    val result = DateParser.parseDate("Mon Apr 23 00:00:00 EST 1984/Sun Apr 29 00:00:00 EST 1984")
    expectResult(false){ result.isEmpty }
    expectResult("1984"){ result.get.startYear }
    expectResult("1984"){ result.get.endYear }
    expectResult(false){ result.get.singleDate }
  }

  test("Fri Aug 12 15:19:20 EST 2011"){
      val result = DateParser.parseDate("Fri Aug 12 15:19:20 EST 2011")
      expectResult(false){ result.isEmpty }
      expectResult("2011"){ result.get.startYear }
      expectResult("2011"){ result.get.endYear }
      expectResult(true){ result.get.singleDate }
  }

  test("1982-03-12"){
      val result = DateParser.parseDate("1982-03-12 ")
      expectResult(false){ result.isEmpty }
      expectResult("1982"){ result.get.startYear }
      expectResult("03"){ result.get.startMonth }
      expectResult(true){ result.get.singleDate }
  }

  test("2011-09-13 09:29:08"){
      val result = DateParser.parseDate("2011-09-13 09:29:08")
      expectResult(false){ result.isEmpty }
      expectResult("2011"){ result.get.startYear }
      expectResult("09"){ result.get.startMonth }
      expectResult("13"){ result.get.startDay }
      expectResult(true){ result.get.singleDate }
  }

  test("21-Aug-2005"){
      val result = DateParser.parseDate("21-Aug-2005")
      expectResult(false){ result.isEmpty }
      expectResult("2005"){ result.get.startYear }
      expectResult("08"){ result.get.startMonth }
      expectResult("21"){ result.get.startDay }
      expectResult(true){ result.get.singleDate }
  }

  test("Aug-2005"){
      val result = DateParser.parseDate("Aug-2005")
      expectResult(false){ result.isEmpty }
      expectResult("2005"){ result.get.startYear }
      expectResult("08"){ result.get.startMonth }
      expectResult(true){ result.get.singleDate }
  }

  test("1998-9-30/10-7"){
    val result = DateParser.parseDate("1998-9-30/10-7")
    expectResult(false){ result.isEmpty}
    expectResult("1998"){result.get.startYear}
    expectResult("09"){result.get.startMonth}
    expectResult("10"){result.get.endMonth}
    expectResult("30"){result.get.startDay}
    expectResult("07"){result.get.endDay}
  }

  test("2011-10-31T18:50:00"){
    val result = DateParser.parseDate("2011-10-31T18:50:00")
    expectResult(false){ result.isEmpty}
    expectResult("2011"){result.get.startYear}
    expectResult("10"){result.get.startMonth}
    expectResult("10"){result.get.endMonth}
    expectResult("31"){result.get.startDay}
    expectResult("31"){result.get.endDay}
    expectResult(true){result.get.singleDate}
  }

  test("2011-10-31Z"){
    val result = DateParser.parseDate("2011-10-31Z")
    expectResult(false){ result.isEmpty}
    expectResult("2011"){result.get.startYear}
    expectResult("10"){result.get.startMonth}
    expectResult("10"){result.get.endMonth}
    expectResult("31"){result.get.startDay}
    expectResult("31"){result.get.endDay}
    expectResult(true){result.get.singleDate}
  }

  test("2001-03-14T00:00:00+11:00"){
    val result = DateParser.parseDate("2001-03-14T00:00:00+11:00")
    expectResult(false){result.isEmpty}
    expectResult("2001"){result.get.startYear}
    expectResult("03"){result.get.startMonth}
    expectResult("03"){result.get.endMonth}
    expectResult("14"){result.get.startDay}
    expectResult("14"){result.get.endDay}
    expectResult(true){result.get.singleDate}
  }
  
  test("2001-03-14T00:00:00+1100"){
    val result = DateParser.parseDate("2001-03-14T00:00:00+1100")
    expectResult(false){result.isEmpty}
    expectResult("2001"){result.get.startYear}
    expectResult("03"){result.get.startMonth}
    expectResult("03"){result.get.endMonth}
    expectResult("14"){result.get.startDay}
    expectResult("14"){result.get.endDay}
    expectResult(true){result.get.singleDate}
  }

  test("2001-03-14T00:00:00+11"){
    val result = DateParser.parseDate("2001-03-14T00:00:00+11")
    expectResult(false){result.isEmpty}
    expectResult("2001"){result.get.startYear}
    expectResult("03"){result.get.startMonth}
    expectResult("03"){result.get.endMonth}
    expectResult("14"){result.get.startDay}
    expectResult("14"){result.get.endDay}
    expectResult(true){result.get.singleDate}
  }

  test("2001-03-14T00:00:00-11:00"){
    val result = DateParser.parseDate("2001-03-14T00:00:00-11:00")
    expectResult(false){result.isEmpty}
    expectResult("2001"){result.get.startYear}
    expectResult("03"){result.get.startMonth}
    expectResult("03"){result.get.endMonth}
    expectResult("14"){result.get.startDay}
    expectResult("14"){result.get.endDay}
    expectResult(true){result.get.singleDate}
  }

  test("2001-03-14T00:00:00-1130"){
    val result = DateParser.parseDate("2001-03-14T00:00:00-1130")
    expectResult(false){result.isEmpty}
    expectResult("2001"){result.get.startYear}
    expectResult("03"){result.get.startMonth}
    expectResult("03"){result.get.endMonth}
    expectResult("14"){result.get.startDay}
    expectResult("14"){result.get.endDay}
    expectResult(true){result.get.singleDate}
  }

  test("2001-03-14T00:00:00-05"){
    val result = DateParser.parseDate("2001-03-14T00:00:00-05")
    expectResult(false){result.isEmpty}
    expectResult("2001"){result.get.startYear}
    expectResult("03"){result.get.startMonth}
    expectResult("03"){result.get.endMonth}
    expectResult("14"){result.get.startDay}
    expectResult("14"){result.get.endDay}
    expectResult(true){result.get.singleDate}
  }

  test("Invalid date ranges"){
    expectResult(None){DateParser.parseDate("2014-02-29")}
    expectResult(None){DateParser.parseDate("2013-13-01")}
    expectResult(None){DateParser.parseDate("2013-23-01")}
  }

  test("yyyy-MM-00"){
    val result = DateParser.parseDate("2011-05-00")
    expectResult(false){result.isEmpty}
    expectResult("2011"){result.get.startYear}
    expectResult("05"){result.get.startMonth}
    expectResult("05"){result.get.endMonth}
    expectResult(""){result.get.startDay}
    expectResult(""){result.get.endDay}
    expectResult(true){result.get.singleDate}
  }

  test("2011-02-09 00:39:00.000"){
    val result = DateParser.parseDate("2011-02-09 00:39:00.000")
    expectResult(false){result.isEmpty}
    expectResult("2011"){result.get.startYear}
    expectResult("02"){result.get.startMonth}
    expectResult("09"){result.get.startDay}
    expectResult(true){result.get.singleDate}
  }

  test("Badly formed date - 0 01"){
    val result = DateParser.parseDate("0 01")
    expectResult(true){result.isEmpty}
  }

  test("dd/MM/yy - post 2000"){
    val result = DateParser.parseDate("01/02/01")
    expectResult("2001"){result.get.startYear}
    expectResult("02"){result.get.startMonth}
    expectResult("02"){result.get.endMonth}
    expectResult("01"){result.get.startDay}
    expectResult("01"){result.get.endDay}
    expectResult(true){result.get.singleDate}
  }

  test("dd/MM/yy - pre 2000"){
    val result = DateParser.parseDate("05/02/78")
    expectResult("1978"){result.get.startYear}
    expectResult("02"){result.get.startMonth}
    expectResult("02"){result.get.endMonth}
    expectResult("05"){result.get.startDay}
    expectResult("05"){result.get.endDay}
    expectResult(true){result.get.singleDate}
  }

  test("dd-mm-yyyy/dd-mm-yyyy"){
    val result = DateParser.parseDate("05-02-1978/06-03-1979")
    expectResult("1978"){result.get.startYear}
    expectResult("1979"){result.get.endYear}
    expectResult("02"){result.get.startMonth}
    expectResult("03"){result.get.endMonth}
    expectResult("05"){result.get.startDay}
    expectResult("06"){result.get.endDay}
    expectResult(false){result.get.singleDate}
  }

  test("dd-mm-yyyy & dd-mm-yyyy"){
    val result = DateParser.parseDate("05-02-1978 & 06-03-1979")
    expectResult("1978"){result.get.startYear}
    expectResult("1979"){result.get.endYear}
    expectResult("02"){result.get.startMonth}
    expectResult("03"){result.get.endMonth}
    expectResult("05"){result.get.startDay}
    expectResult("06"){result.get.endDay}
    expectResult(false){result.get.singleDate}
  }

  test("dd-mm-yyyy to dd-mm-yyyy"){
    val result = DateParser.parseDate("05-02-1978 to 06-03-1979")
    expectResult("1978"){result.get.startYear}
    expectResult("1979"){result.get.endYear}
    expectResult("02"){result.get.startMonth}
    expectResult("03"){result.get.endMonth}
    expectResult("05"){result.get.startDay}
    expectResult("06"){result.get.endDay}
    expectResult(false){result.get.singleDate}
  }
}
