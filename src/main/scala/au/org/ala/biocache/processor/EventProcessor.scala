package au.org.ala.biocache.processor

import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import org.apache.commons.lang.time.{DateFormatUtils, DateUtils}
import java.util.{GregorianCalendar, Date}
import org.apache.commons.lang.StringUtils
import au.org.ala.biocache.parser.DateParser
import au.org.ala.biocache.model.{QualityAssertion, FullRecord}
import au.org.ala.biocache.vocab.{DatePrecision, AssertionStatus, AssertionCodes}
import au.org.ala.biocache.util.{DateUtil, StringHelper}

/**
 * Processor for event (date) information.
 */
class EventProcessor extends Processor {

  import StringHelper._
  import AssertionCodes._
  import AssertionStatus._

  val logger = LoggerFactory.getLogger("EventProcessor")

  /**
   * Validate the supplied number using the supplied function.
   */
  def validateNumber(number: String, f: (Int => Boolean)): (Int, Boolean) = {
    try {
      if (number != null) {
        val parsedNumber = number.toInt
        (parsedNumber, f(parsedNumber))
      } else {
        (-1, false)
      }
    } catch {
      case e: NumberFormatException => (-1, false)
    }
  }

  /**
   * Date parsing
   *
   * TODO needs splitting into several methods
   */
  def process(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord]=None): Array[QualityAssertion] = {
    var assertions = new ArrayBuffer[QualityAssertion]
    if ((raw.event.day == null || raw.event.day.isEmpty)
      && (raw.event.month == null || raw.event.month.isEmpty)
      && (raw.event.year == null || raw.event.year.isEmpty)
      && (raw.event.eventDate == null || raw.event.eventDate.isEmpty)
      && (raw.event.verbatimEventDate == null || raw.event.verbatimEventDate.isEmpty)
    ){
      assertions += QualityAssertion(MISSING_COLLECTION_DATE, "No date information supplied")
    } else {

      var date: Option[java.util.Date] = None
      val currentYear = DateUtil.getCurrentYear
      var comment = ""
      var addPassedInvalidCollectionDate = true
      var dateComplete = false

      var (year, validYear) = validateNumber(raw.event.year, {
        year => year > 0 && year <= currentYear
      })
      var (month, validMonth) = validateNumber(raw.event.month, {
        month => month >= 1 && month <= 12
      })
      var (day, validDay) = validateNumber(raw.event.day, {
        day => day >= 1 && day <= 31
      })
      //check month and day not transposed
      if (!validMonth && raw.event.month.isInt && raw.event.day.isInt) {
        //are day and month transposed?
        val monthValue = raw.event.month.toInt
        val dayValue = raw.event.day.toInt
        if (monthValue > 12 && dayValue < 12) {
          month = dayValue
          day = monthValue
          assertions += QualityAssertion(DAY_MONTH_TRANSPOSED, "Assume day and month transposed")
          //assertions += QualityAssertion(INVALID_COLLECTION_DATE,PASSED) //this one is not applied
          validMonth = true
        } else {
          assertions += QualityAssertion(INVALID_COLLECTION_DATE, "Invalid month supplied")
          addPassedInvalidCollectionDate = false
          assertions += QualityAssertion(DAY_MONTH_TRANSPOSED, PASSED) //this one has been tested and does not apply
        }
      }

      //TODO need to check for other months
      if (day == 0 || day > 31) {
        assertions += QualityAssertion(INVALID_COLLECTION_DATE, "Invalid day supplied")
        addPassedInvalidCollectionDate = false
      }

      //check for sensible year value
      if (year > 0) {
        val (newComment,newValidYear,newYear) = runYearValidation(year,currentYear,day,month)
        comment = newComment
        validYear = newValidYear
        year = newYear

        if (StringUtils.isNotEmpty(comment)){
          assertions += QualityAssertion(INVALID_COLLECTION_DATE, comment)
          addPassedInvalidCollectionDate=false
        }
      }

      var validDayMonthYear = validYear && validDay && validMonth

      //construct
      if (validDayMonthYear) {
        try {

          val calendar = new GregorianCalendar(
            year.toInt,
            month.toInt - 1,
            day.toInt
          )
          //don't allow the calendar to be lenient we want exceptions with incorrect dates
          calendar.setLenient(false)
          date = Some(calendar.getTime)
          dateComplete = true
        } catch {
          case e: Exception => {
            validDayMonthYear = false
            comment = "Invalid year, day, month"
            assertions += QualityAssertion(INVALID_COLLECTION_DATE, comment)
            addPassedInvalidCollectionDate = false
          }
        }
      }

      //set the processed values
      if (validYear) processed.event.year = year.toString
      if (validMonth) processed.event.month = String.format("%02d", int2Integer(month)) //NC ensure that a month is 2 characters long
      if (validDay) processed.event.day = day.toString
      if (!date.isEmpty) processed.event.eventDate = DateFormatUtils.format(date.get, "yyyy-MM-dd")

      //deal with event date if we dont have separate day, month, year fields
      if (date.isEmpty && raw.event.eventDate != null && !raw.event.eventDate.isEmpty) {
        val parsedDate = DateParser.parseDate(raw.event.eventDate)
        if (!parsedDate.isEmpty) {
          //set processed values
          processed.event.eventDate = parsedDate.get.startDate
          if (!parsedDate.get.endDate.equals(parsedDate.get.startDate)) processed.event.eventDateEnd = parsedDate.get.endDate
          processed.event.day = parsedDate.get.startDay
          processed.event.month = parsedDate.get.startMonth
          processed.event.year = parsedDate.get.startYear
          //set the valid year if one was supplied in the eventDate
          if(parsedDate.get.startYear != "") {
            val(newComment,newValidYear,newYear) = runYearValidation(parsedDate.get.startYear.toInt, currentYear, if(parsedDate.get.startDay =="") 0 else parsedDate.get.startDay.toInt, if(parsedDate.get.startMonth == "") 0 else parsedDate.get.startMonth.toInt)
            comment = newComment
            validYear = newValidYear
            year = newYear
          }

          if(StringUtils.isNotBlank(parsedDate.get.startDate)){
            //we have a complete date
            dateComplete = true
          }

          if (DateUtil.isFutureDate(parsedDate.get)) {
            assertions += QualityAssertion(INVALID_COLLECTION_DATE, "Future date supplied")
            addPassedInvalidCollectionDate = false
          }
        }
      } else if (raw.event.eventDate != null && !raw.event.eventDate.isEmpty) {
        //look for an end date
        val parsedDate = DateParser.parseDate(raw.event.eventDate)
        if (!parsedDate.isEmpty) {
          //what happens if d m y make the eventDate and eventDateEnd is parsed?
          if (!parsedDate.get.endDate.equals(parsedDate.get.startDate)) processed.event.eventDateEnd = parsedDate.get.endDate
        }
      }

      //deal with verbatim date
      if (date.isEmpty && raw.event.verbatimEventDate != null && !raw.event.verbatimEventDate.isEmpty) {
        val parsedDate = DateParser.parseDate(raw.event.verbatimEventDate)
        if (!parsedDate.isEmpty) {
          //set processed values
          processed.event.eventDate = parsedDate.get.startDate
          if (!parsedDate.get.endDate.equals(parsedDate.get.startDate)) processed.event.eventDateEnd = parsedDate.get.endDate
          processed.event.day = parsedDate.get.startDay
          processed.event.month = parsedDate.get.startMonth
          processed.event.year = parsedDate.get.startYear

          //set the valid year if one was supplied in the verbatimEventDate
          if(parsedDate.get.startYear != "") {
            val(newComment,newValidYear,newYear) = runYearValidation(parsedDate.get.startYear.toInt, currentYear,if(parsedDate.get.startDay =="") 0 else parsedDate.get.startDay.toInt, if(parsedDate.get.startMonth =="") 0 else parsedDate.get.startMonth.toInt)
            comment = newComment
            validYear = newValidYear
            year = newYear
          }

          if(StringUtils.isNotBlank(parsedDate.get.startDate)){
            //we have a complete date
            dateComplete=true
          }

          if (DateUtil.isFutureDate(parsedDate.get)) {
            assertions += QualityAssertion(INVALID_COLLECTION_DATE, "Future date supplied")
            addPassedInvalidCollectionDate = false
          }
        }
      } else if ((processed.event.eventDateEnd == null || processed.event.eventDateEnd.isEmpty()) &&
        raw.event.verbatimEventDate != null && !raw.event.verbatimEventDate.isEmpty) {
        //look for an end date
        val parsedDate = DateParser.parseDate(raw.event.verbatimEventDate)
        if (!parsedDate.isEmpty && !parsedDate.get.endDate.equals(parsedDate.get.startDate)) {
          //what happens if d m y make the eventDate and eventDateEnd is parsed?
          if (!parsedDate.get.endDate.equals(parsedDate.get.startDate)) processed.event.eventDateEnd = parsedDate.get.endDate
        }
      }

      //if invalid date, add assertion
      if (!validYear && (processed.event.eventDate == null || processed.event.eventDate == "" || comment != "")) {
        assertions += QualityAssertion(INVALID_COLLECTION_DATE, comment)
        addPassedInvalidCollectionDate = false
      }

      //chec for future date
      if (!date.isEmpty && date.get.after(new Date())) {
        assertions += QualityAssertion(INVALID_COLLECTION_DATE, "Future date supplied")
        addPassedInvalidCollectionDate = false
      }

      //check to see if we need add a passed test for the invalid collection dates
      if(addPassedInvalidCollectionDate)
        assertions += QualityAssertion(INVALID_COLLECTION_DATE, PASSED)

      if(dateComplete){
        //add a pass condition for this test
        assertions += QualityAssertion(INCOMPLETE_COLLECTION_DATE, PASSED)
      } else{
        //incomplete date
        assertions += QualityAssertion(INCOMPLETE_COLLECTION_DATE, "The supplied collection date is not complete")
      }
    }

    //now process the other dates
    processOtherDates(raw, processed, assertions)

    //check for the "first" of month,year,century
    processFirstDates(raw, processed, assertions)

    //validate against date precision
    checkPrecision(raw, processed, assertions)

    assertions.toArray
  }

  def runYearValidation(rawyear:Int, currentYear:Int, day:Int=0, month:Int=0):(String,Boolean,Int)={
    var validYear =true
    var comment=""
    var year = rawyear
    if (year > 0) {
      if (year < 100) {
        //parse 89 for 1989
        if (year > currentYear % 100) {
          // Must be in last century
          year += ((currentYear / 100) - 1) * 100
        } else {
          // Must be in this century
          year += (currentYear / 100) * 100

          //although check that combined year-month-day isnt in the future
          if (day != 0 && month != 0) {
            val date = DateUtils.parseDate(year.toString + String.format("%02d", int2Integer(month)) + day.toString, Array("yyyyMMdd"))
            if (date.after(new Date())) {
              year -= 100
            }
          }
        }
      } else if (year >= 100 && year < 1600) {
        year = -1
        validYear = false
        comment = "Year out of range"
      } else if (year > DateUtil.getCurrentYear) {
        year = -1
        validYear = false
        comment = "Future year supplied"
        //assertions + QualityAssertion(INVALID_COLLECTION_DATE,comment)
      } else if (year == 1788 && month == 1 && day == 26) {
        //First fleet arrival date indicative of a null date.
        validYear = false
        comment = "First Fleet arrival implies a null date"
      }

    }
    (comment,validYear,year)
  }

  def processFirstDates(raw:FullRecord, processed:FullRecord, assertions:ArrayBuffer[QualityAssertion]){
    //check to see if the date is the first of a month
    if(processed.event.day == "1" || processed.event.day == "01"){
      assertions += QualityAssertion(FIRST_OF_MONTH)
      //check to see if the date is the first of the year
      if(processed.event.month == "01" || processed.event.month == "1") {
        assertions += QualityAssertion(FIRST_OF_YEAR)
        //check to see if the date is the first of the century
        if(processed.event.year != null){
          val (year, validYear) = validateNumber(processed.event.year, {
            year => year > 0
          })
          if(validYear && year % 100 == 0){
            assertions += QualityAssertion(FIRST_OF_CENTURY)
          } else {
            //the date is NOT the first of the century
            assertions += QualityAssertion(FIRST_OF_CENTURY, PASSED)
          }
        }
      } else if(processed.event.month != null) {
        //the date is not the first of the year
        assertions += QualityAssertion(FIRST_OF_YEAR, PASSED)
      }
    } else if(processed.event.day != null) {
      //the date is not the first of the month
      assertions += QualityAssertion(FIRST_OF_MONTH, PASSED)
    }
  }

  /**
   * processed the other dates for the occurrence including performing data checks
    *
    * @param raw
   * @param processed
   * @param assertions
   */
  def processOtherDates(raw:FullRecord, processed:FullRecord, assertions:ArrayBuffer[QualityAssertion]){
    //process the "modified" date for the occurrence - we want all modified dates in the same format so that we can index on them...
    if (raw.occurrence.modified != null) {
      val parsedDate = DateParser.parseDate(raw.occurrence.modified)
      if (parsedDate.isDefined) {
        processed.occurrence.modified = parsedDate.get.startDate
      }
    }

    if (raw.identification.dateIdentified != null) {
      val parsedDate = DateParser.parseDate(raw.identification.dateIdentified)
      if (parsedDate.isDefined)
        processed.identification.dateIdentified = parsedDate.get.startDate
    }

    if (raw.location.georeferencedDate != null || raw.miscProperties.containsKey("georeferencedDate")){
      def rawdate = if (raw.location.georeferencedDate != null) raw.location.georeferencedDate else raw.miscProperties.get("georeferencedDate")
      val parsedDate = DateParser.parseDate(rawdate)
      if (parsedDate.isDefined){
        processed.location.georeferencedDate = parsedDate.get.startDate
      }
    }

    if (StringUtils.isNotBlank(processed.event.eventDate)){
      val eventDate = DateParser.parseStringToDate(processed.event.eventDate).get
      //now test if the record was identified before it was collected
      if(StringUtils.isNotBlank(processed.identification.dateIdentified)){
        if (DateParser.parseStringToDate(processed.identification.dateIdentified).get.before(eventDate)){
          //the record was identified before it was collected !!
          assertions += QualityAssertion(ID_PRE_OCCURRENCE, "The records was identified before it was collected")
        } else {
          assertions += QualityAssertion(ID_PRE_OCCURRENCE, PASSED)
        }
      }

      //now check if the record was georeferenced after the collection date
      if(StringUtils.isNotBlank(processed.location.georeferencedDate)) {
        if(DateParser.parseStringToDate(processed.location.georeferencedDate).get.after(eventDate)){
          //the record was not georeference when it was collected!!
          assertions += QualityAssertion(GEOREFERENCE_POST_OCCURRENCE, "The record was not georeferenced when it was collected")
        } else {
          assertions += QualityAssertion(GEOREFERENCE_POST_OCCURRENCE, PASSED)
        }
      }
    }
  }

  /**
    * Check the precision of the supplied date and alter the processed date
    *
    * @param raw
    * @param processed
    * @param assertions
    */
  def checkPrecision(raw:FullRecord, processed:FullRecord, assertions:ArrayBuffer[QualityAssertion]){

    if(StringUtils.isNotBlank(raw.event.datePrecision) && StringUtils.isNotBlank(processed.event.eventDate)){
      val matchedTerm = DatePrecision.matchTerm(raw.event.datePrecision)
      if(!matchedTerm.isEmpty){
        val term = matchedTerm.get
        processed.event.datePrecision = term.canonical

        if (term.canonical.equalsIgnoreCase(MONTH_PRECISION)){
          //is the processed date in yyyy-MM format
          reformatToPrecision(processed, "yyyy-MM", true, false, false)
        }
        if (term.canonical.equalsIgnoreCase(YEAR_PRECISION)){
          //is the processed date in yyyy format
          reformatToPrecision(processed, "yyyy", true, true, false)
        }
//        if (term.canonical.equalsIgnoreCase(MONTH_RANGE_PRECISION)){
//          is the processed date in yyyy-MM format
//          reformatToPrecision(processed, "yyyy-MM", true, false, false)
//        }
//        if (term.canonical.equalsIgnoreCase(YEAR_RANGE_PRECISION)){
//          is the processed date in yyyy format
//          reformatToPrecision(processed, "yyyy", true, true, true)
//        }
      }
    }
  }

  /**
    * TODO handle all the permutations of year and month ranges.
    *
    * @param processed
    * @param format
    * @param nullifyDay
    * @param nullifyMonth
    * @param nullifyYear
    */
  def reformatToPrecision(processed:FullRecord, format:String, nullifyDay:Boolean, nullifyMonth:Boolean, nullifyYear:Boolean): Unit ={

    val parsedDate = DateParser.parseDate(processed.event.eventDate)
    if(!parsedDate.isEmpty) {

      if(parsedDate.get.singleDate && parsedDate.get.parsedStartDate != null){
        try {
          processed.event.eventDate = DateFormatUtils.format(parsedDate.get.parsedStartDate, format)
        } catch {
          case e:Exception => logger.error("Problem reformatting date to new precision")
        }
      }

      if(parsedDate.get.singleDate) {
        if (nullifyDay) {
          processed.event.day = null
        }
        if (nullifyMonth) {
          processed.event.month = null
        }
        if (nullifyYear) {
          processed.event.year = null
        }
      }
    }
  }

  val DAY_RANGE_PRECISION = "day range"
  val MONTH_RANGE_PRECISION = "month range"
  val YEAR_RANGE_PRECISION = "year range"

  val DAY_PRECISION = "day"
  val MONTH_PRECISION = "month"
  val YEAR_PRECISION = "year"

  def getName = "event"
}
