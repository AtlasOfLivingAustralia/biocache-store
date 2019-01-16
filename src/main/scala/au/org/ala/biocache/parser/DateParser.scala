package au.org.ala.biocache.parser

import org.apache.commons.lang.time.DateUtils
import org.apache.commons.lang.time.DateFormatUtils
import org.slf4j.LoggerFactory
import org.slf4j.Logger
import java.util.Date
import java.text.ParseException
import scala.Predef._
import au.org.ala.biocache.Config
import au.org.ala.biocache.util.DateUtil
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatterBuilder
import java.util.concurrent.Callable
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import java.util.Locale
import java.time.format.DateTimeFormatterBuilder
import java.time.format.ResolverStyle
import com.google.common.cache.CacheBuilder
import java.time.LocalDate
import java.time.ZoneId
import java.time.LocalDateTime
import java.time.ZonedDateTime
import java.time.Month
import java.time.Year

/**
 * Date parser that uses scala extractors to handle the different formats.
 */
object DateParser {

  final val logger: Logger = LoggerFactory.getLogger("DateParser")

  val dateFormatCache = CacheBuilder.newBuilder().maximumSize(Config.dateFormatCacheSize).build[Tuple4[String, Boolean, Boolean, Boolean], DateTimeFormatter]()

  def fromLocalDate(toConvert: Option[LocalDate]): Option[Date] = {
    toConvert match {
      case Some(toConvert) => {
        Some(Date.from(toConvert.atStartOfDay(ZoneId.of("UTC")).toInstant()))
      }
      case None => None
    }
  }
  
  def fromLocalDateTime(toConvert: Option[LocalDateTime]): Option[Date] = {
    toConvert match {
      case Some(toConvert) => {
        Some(Date.from(toConvert.atZone(ZoneId.of("UTC")).toInstant()))
      }
      case None => None
    }
  }
  
  def fromOffsetDateTime(toConvert: Option[OffsetDateTime]): Option[Date] = {
    toConvert match {
      case Some(toConvert) => {
        Some(Date.from(toConvert.atZoneSameInstant(ZoneId.of("UTC")).toInstant()))
      }
      case None => None
    }
  }
  
  def fromZonedDateTime(toConvert: Option[ZonedDateTime]): Option[Date] = {
    toConvert match {
      case Some(toConvert) => {
        Some(Date.from(toConvert.withZoneSameInstant(ZoneId.of("UTC"))toInstant()))
      }
      case None => None
    }
  }
  
  def localDateMatches(dateValue: String, inputFormat: DateTimeFormatter): Boolean = {
    try {
      LocalDate.parse(dateValue, inputFormat)
      true
    } catch {
      case _:Exception => false
    }
  }
  
  def localDateTimeMatches(dateValue: String, inputFormat: DateTimeFormatter): Boolean = {
    try {
      LocalDateTime.parse(dateValue, inputFormat)
      true
    } catch {
      case _:Exception => false
    }
  }
  
  def offsetDateTimeMatches(dateValue: String, inputFormat: DateTimeFormatter): Boolean = {
    try {
      OffsetDateTime.parse(dateValue, inputFormat)
      true
    } catch {
      case _:Exception => false
    }
  }
  
  def zonedDateTimeMatches(dateValue: String, inputFormat: DateTimeFormatter): Boolean = {
    try {
      ZonedDateTime.parse(dateValue, inputFormat)
      true
    } catch {
      case _:Exception => false
    }
  }
  
  def dateMatches(dateValue: String, inputFormat: DateTimeFormatter): Boolean = {
    try {
      inputFormat.parse(dateValue)
      true
    } catch {
      case _:Exception => false
    }
  }

  def newDateFormat(formatPattern: String, defaultYear: Boolean = false, defaultMonth: Boolean = false, defaultDay: Boolean = false, appendOffsetParser: Boolean = false): DateTimeFormatter = {
    
    val cacheKey = (formatPattern, defaultMonth, defaultDay, appendOffsetParser)
    
    dateFormatCache.get(cacheKey, new Callable[DateTimeFormatter]() {
      def call(): DateTimeFormatter = {
        var builder = new DateTimeFormatterBuilder().parseCaseInsensitive().appendPattern(formatPattern)
        
        if (appendOffsetParser) {
          builder = appendOffsetParsing(builder)
        }
        
        if (defaultYear) {
          builder = builder.parseDefaulting(ChronoField.YEAR, 1)
        }
        
        if (defaultMonth) {
          builder = builder.parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
        }
        
        if (defaultDay) {
          builder = builder.parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
        }
        
        builder.toFormatter(Locale.US).withResolverStyle(ResolverStyle.STRICT)
      }
    })
  }
  
  def newTwoDigitYearDateFormat(formatPatternStart: String, formatPatternEnd: String, twoDigitYearCutoff: Int = 1920, defaultMonth: Boolean = false, defaultDay: Boolean = false, appendOffsetParser: Boolean = false): DateTimeFormatter = {
    
    val cacheKey = (formatPatternStart + "[uuuu][uu]" + formatPatternEnd, defaultMonth, defaultDay, appendOffsetParser)
    dateFormatCache.get(cacheKey, new Callable[DateTimeFormatter]() {
      def call(): DateTimeFormatter = {
        var builder = new DateTimeFormatterBuilder().parseCaseInsensitive()
        
        if(!formatPatternStart.isEmpty()) {
          builder = builder.appendPattern(formatPatternStart)
        }
        
        // Transparently allow for two and four digit years, with a customisable cutoff year
        builder = builder.optionalStart()
            .appendPattern("uuuu")
            .optionalEnd()
            .optionalStart()
            .appendValueReduced(ChronoField.YEAR, 2, 2, twoDigitYearCutoff)
            .optionalEnd()
            
        if (!formatPatternEnd.isEmpty()) {
          builder = builder.appendPattern(formatPatternEnd)
        }
            
        if (appendOffsetParser) {
          builder = appendOffsetParsing(builder)
        }
        
        if (defaultMonth) {
          builder = builder.parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
        }
        
        if (defaultDay) {
          builder = builder.parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
        }
        
        builder.toFormatter(Locale.US).withResolverStyle(ResolverStyle.STRICT)
      }
    })
  }
  
  def appendDateParsing(formatter: DateTimeFormatterBuilder, optional: Boolean = false): DateTimeFormatterBuilder = {
    var resultFormatter = formatter
    if (optional) {
      resultFormatter = resultFormatter.optionalStart()
    }
    resultFormatter = resultFormatter.append(DateTimeFormatter.ISO_LOCAL_DATE)
    if (optional) {
      resultFormatter = resultFormatter.optionalEnd()
    }
    resultFormatter
  }
  
  def appendTimeParsing(formatter: DateTimeFormatterBuilder, optional: Boolean = true): DateTimeFormatterBuilder = {
    var resultFormatter = formatter
    if (optional) {
      resultFormatter = resultFormatter.optionalStart()
    }
    resultFormatter = resultFormatter.append(DateTimeFormatter.ISO_LOCAL_TIME)
    if (optional) {
      resultFormatter = resultFormatter.optionalEnd()
    }
    resultFormatter
  }
  
  def appendOffsetParsing(formatter: DateTimeFormatterBuilder): DateTimeFormatterBuilder = {
    val resultFormatter = 
      formatter
               .optionalStart().appendOffset("+HH:MM", "+00:00").optionalEnd()
               .optionalStart().appendOffset("+HHMM", "+0000").optionalEnd()
               .optionalStart().appendOffset("+HH", "Z").optionalEnd()
    resultFormatter
  }
  
  val YEAR_TO_LOCAL_DATE = newDateFormat("uuuu", false, true, true)
  val YEAR_MONTH_TO_LOCAL_DATE = newDateFormat("uuuu-MM", false, false, true)
  val MONTH_TO_LOCAL_DATE = newDateFormat("MM", true, false, true)
  val DAY_TO_LOCAL_DATE = newDateFormat("dd", true, true, false)
  val MONTH_DAY_TO_LOCAL_DATE = newDateFormat("MM-dd", true, false, false)
  val YEAR_MONTH = newDateFormat("uuuu-MM")
  val YEAR = newDateFormat("uuuu")
  val MONTH = newDateFormat("MM")
  val DAY = newDateFormat("dd")
  val SHORT_MONTH_HYPHEN_DAY = newDateFormat("M-d", true, false, false)
//  val SHORT_MONTH_HYPHEN_DAY = {
//    var result = new DateTimeFormatterBuilder().parseCaseInsensitive()
//    result.appendPattern("M-d")
//    // Need to provide a default for the year or a LocalDate won't be created
//    result = result.parseDefaulting(ChronoField.YEAR, 1)
//    result.toFormatter(Locale.US).withResolverStyle(ResolverStyle.STRICT)
//  }
  val OFFSET_DATE_OPTIONAL_TIME = { 
    var result = new DateTimeFormatterBuilder().parseCaseInsensitive()
    result = appendDateParsing(result, false)
    result = result.optionalStart().appendLiteral('T').optionalEnd()
    result = appendTimeParsing(result, true)
    result = appendOffsetParsing(result)
    result.toFormatter(Locale.US).withResolverStyle(ResolverStyle.STRICT)
  }
  val NON_ISO_OFFSET_DATE_OPTIONAL_TIME = { 
    var result = new DateTimeFormatterBuilder().parseCaseInsensitive()
    result = appendDateParsing(result, false)
    // Alternative that uses an optional space character instead of the standard ISO "T" character
    result = result.optionalStart().appendLiteral(' ').optionalEnd()
    result = appendTimeParsing(result, true)
    result = appendOffsetParsing(result)
    result.toFormatter(Locale.US).withResolverStyle(ResolverStyle.STRICT)
  }

  /**
    * Parse the supplied date string into an event date. This method catches errors and will return a None
    * if the string is unparseable.
    *
    * @param dateStr
    * @param maxYear
    * @param minYear
    * @return an event date if parseable.
    */
  def parseDate(dateStr: String, maxYear: Option[Int] = None, minYear: Option[Int] = None): Option[EventDate] = {

    if(dateStr == null)
      return None

    val dateStrNormalised = {
      val x = dateStr.trim
      if(x.startsWith("/") || x.startsWith("-")){
        x.substring(1)
      } else {
        x
      }
    }
    //assume ISO
    val eventDateWithOption = parseISODate(dateStrNormalised)

    //if max year set, validate
    eventDateWithOption match {
      case Some(eventDate) => {
        if (!isValid(eventDate)) {
          val secondAttempt = parseNonISODate(dateStrNormalised)
          if(!secondAttempt.isEmpty && isValid(secondAttempt.get)){
            secondAttempt
          } else {
            parseNonISOTruncatedYearDate(dateStrNormalised)
          }
        } else {
          eventDateWithOption
        }
      }
      case None => {
        //NQ 2014-04-07: second attempt - needed because when strict parsing is enabled non iso formats will fall down to here
        val secondAttempt = parseNonISODate(dateStrNormalised)
        if(!secondAttempt.isEmpty && isValid(secondAttempt.get)){
          secondAttempt
        } else {
          parseNonISOTruncatedYearDate(dateStrNormalised)
        }
      }
    }
  }

  /**
   * Parses a ISO Date time string to a Date
   */
  def parseStringToDate(date: String): Option[Date] = {
    try {
      if(date == "") {
        None
      } else if (localDateMatches(date, DateTimeFormatter.ISO_LOCAL_DATE)) {
        fromLocalDate(Some(LocalDate.parse(date, DateTimeFormatter.ISO_LOCAL_DATE)))
      } else if (localDateMatches(date, DateParser.YEAR_TO_LOCAL_DATE)) {
        fromLocalDate(Some(LocalDate.parse(date, DateParser.YEAR_TO_LOCAL_DATE)))
      } else if (localDateMatches(date, DateParser.YEAR_MONTH_TO_LOCAL_DATE)) {
        fromLocalDate(Some(LocalDate.parse(date, DateParser.YEAR_MONTH_TO_LOCAL_DATE)))
      } else if (localDateTimeMatches(date, DateTimeFormatter.ISO_LOCAL_DATE_TIME)) {
        // Add 'Z' to the original string so LocalDateTime instances are parsed as if they were all in UTC
        fromZonedDateTime(Some(ZonedDateTime.parse(date + "Z", DateTimeFormatter.ISO_DATE_TIME)))
      } else if (localDateTimeMatches(date, DateParser.NON_ISO_OFFSET_DATE_OPTIONAL_TIME)) {
        fromZonedDateTime(Some(ZonedDateTime.parse(date + "Z", DateParser.NON_ISO_OFFSET_DATE_OPTIONAL_TIME)))
      } else if (zonedDateTimeMatches(date, DateTimeFormatter.ISO_DATE_TIME)) {
        fromZonedDateTime(Some(ZonedDateTime.parse(date, DateTimeFormatter.ISO_DATE_TIME)))
      } else if (offsetDateTimeMatches(date, DateParser.OFFSET_DATE_OPTIONAL_TIME)) {
        fromOffsetDateTime(Some(OffsetDateTime.parse(date, DateParser.OFFSET_DATE_OPTIONAL_TIME)))
      } else if (offsetDateTimeMatches(date, DateParser.NON_ISO_OFFSET_DATE_OPTIONAL_TIME)) {
        fromOffsetDateTime(Some(OffsetDateTime.parse(date, DateParser.NON_ISO_OFFSET_DATE_OPTIONAL_TIME)))
      } else {
        None
      }
    } catch {
      case _:Exception => None
    }
  }

  /**
   * Handles these formats (taken from Darwin Core specification):
   *
   * 1963-03-08T14:07-0600" is 8 Mar 1963 2:07pm in the time zone six hours earlier than UTC,
   * "2009-02-20T08:40Z" is 20 Feb 2009 8:40am UTC, "1809-02-12" is 12 Feb 1809,
   * "1906-06" is Jun 1906, "1971" is just that year,
   * "2007-03-01T13:00:00Z/2008-05-11T15:30:00Z" is the interval between 1 Mar 2007 1pm UTC and
   * 11 May 2008 3:30pm UTC, "2007-11-13/15" is the interval between 13 Nov 2007 and 15 Nov 2007
   *
   * 2005-06-12 00:00:00.0/2005-06-12 00:00:00.0 
   */
  def parseISODate(date: String): Option[EventDate] = {

    date match {
      case ISOSingleYear(date) => Some(date)
      case ISOMonthDate(date) => Some(date)
      case ISOSingleDate(date) => Some(date)
      case ISOWithMonthNameDate(date) => Some(date)
      case ISODateRange(date) => Some(date)
      case ISODayDateRange(date) => Some(date)
      case ISODayMonthRange(date)=>Some(date)
      case ISOMonthYearDateRange(date) => Some(date)
      case ISOMonthDateRange(date) => Some(date)
      case ISOYearRange(date) => Some(date)
      case ISODateTimeRange(date) => Some(date)
      case ISOVerboseDateTimeRange(date) => Some(date)
      case ISOVerboseDateTime(date) => Some(date)
      case _ => None
    }
  }

  def parseNonISODate(date: String): Option[EventDate] = {

    date match {
      case NonISOSingleDate(date) => Some(date)
      case NonISODateRange(date) => Some(date)
      case _ => None
    }
  }

  def parseNonISOTruncatedYearDate(date: String): Option[EventDate] = {

    date match {
      case NonISOTruncatedYearDate(date) => Some(date)
      case _ => None
    }
  }

  def isValid(eventDate: EventDate): Boolean = {

    try {
      val currentYear = DateUtil.getCurrentYear

      if (eventDate.startYear != null) {
        val year = eventDate.startYear.toInt
        if (year > currentYear) return false
      }

      if (eventDate.endYear != null) {
        val year = eventDate.endYear.toInt
        if (year < 1600) return false
      }

      if (eventDate.startYear != null && eventDate.endYear != null) {
        val startYear = eventDate.startYear.toInt
        val endYear = eventDate.endYear.toInt
        if (startYear > endYear) return false
      }
      true
    } catch {
      case e: Exception => {
        logger.debug("Exception thrown parsing date: " + eventDate, e)
        false
      }
    }
  }

  def parseByFormat(str: String, parsedFormats: Array[DateTimeFormatter]): Option[LocalDate] = {
    val matchedFormat = parsedFormats.find(nextFormatter => DateParser.dateMatches(str, nextFormatter))
    if(matchedFormat.isDefined) {
      val nextParsedDate = LocalDate.parse(str, matchedFormat.get)
      Some(nextParsedDate)
    } else {
      None
    }
  }
  
  def parseISOOrFormats(str: String, parsedFormats: Array[DateTimeFormatter]): Option[LocalDate] = {
    val eventDateParsed: Option[LocalDate] = if (DateParser.localDateMatches(str, DateTimeFormatter.ISO_LOCAL_DATE)) {
      Some(LocalDate.parse(str, DateTimeFormatter.ISO_LOCAL_DATE))
    } else if (DateParser.localDateTimeMatches(str, DateTimeFormatter.ISO_LOCAL_DATE_TIME)) {
      Some(LocalDateTime.parse(str, DateTimeFormatter.ISO_LOCAL_DATE_TIME).toLocalDate())
    } else if (DateParser.localDateMatches(str, DateTimeFormatter.ISO_DATE)) {
      Some(LocalDate.parse(str, DateTimeFormatter.ISO_DATE))
    } else if (DateParser.localDateMatches(str, DateTimeFormatter.ISO_OFFSET_DATE)) {
      Some(LocalDate.parse(str, DateTimeFormatter.ISO_OFFSET_DATE))
    } else if (DateParser.offsetDateTimeMatches(str, DateTimeFormatter.ISO_OFFSET_DATE_TIME)) {
      Some(OffsetDateTime.parse(str, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toLocalDate())
    } else if (DateParser.zonedDateTimeMatches(str, DateTimeFormatter.ISO_ZONED_DATE_TIME)) {
      Some(ZonedDateTime.parse(str, DateTimeFormatter.ISO_ZONED_DATE_TIME).toLocalDate())
    } else if (DateParser.offsetDateTimeMatches(str, DateParser.OFFSET_DATE_OPTIONAL_TIME)) {
      Some(OffsetDateTime.parse(str, DateParser.OFFSET_DATE_OPTIONAL_TIME).toLocalDate())
    } else {
      DateParser.parseByFormat(str, parsedFormats)
    }
    eventDateParsed
  }

}

case class EventDate(parsedStartDate: Date, startDate: String, startDay: String, startMonth: String, startYear: String,
                     parsedEndDate: Date, endDate: String, endDay: String, endMonth: String, endYear: String, singleDate: Boolean)

/** Extractor for the format uuuu-MM-dd */
object ISOWithMonthNameDate {

  def baseFormats = Array("uuuu-MMMM-dd", "uuuu-MMMM-dd'T'hh:mm:ss","uuuu-MMMM-dd'T'hh:mm:ss.SSS",  "uuuu-MMMM-dd'T'HH:mm:ss","uuuu-MMMM-dd'T'HH:mm:ss.SSS", "uuuu-MMMM-dd'T'hh:mm-ss", "uuuu-MMMM-dd'T'HH:mm-ss", "uuuu-MMMM-dd'T'hh:mm'Z'", "uuuu-MMMM-dd'T'HH:mm'Z'")

  def baseParseOffsets = false
  
  def formats = baseFormats

  def parseOffsets = baseParseOffsets

  def parsedFormats = formats.map(f => DateParser.newDateFormat(f, false, false, false, true))

  /**
   * Extraction method
   */
  def unapply(str: String): Option[EventDate] = {
    try {
      val eventDateParsed: Option[LocalDate] = DateParser.parseByFormat(str, parsedFormats)
      
      if (eventDateParsed.isDefined) {
        val eventDateSerialised = eventDateParsed.get.format(DateTimeFormatter.ISO_LOCAL_DATE)
        val startDate = eventDateSerialised
        val endDate = eventDateSerialised
        val startYear, endYear = eventDateParsed.get.format(DateParser.YEAR)
        val startMonth, endMonth = eventDateParsed.get.format(DateParser.MONTH)
        val startDay, endDay = eventDateParsed.get.format(DateParser.DAY)
        val eventDateLegacyClass = DateParser.fromLocalDate(eventDateParsed).get

        Some(EventDate(eventDateLegacyClass, startDate, startDay, startMonth, startYear, eventDateLegacyClass, endDate, endDay,
          endMonth: String, endYear, true))
      } else {
        None
      }
    } catch {
      case e: Exception => None
    }
  }
}

/** Extractor for the format uuuu */
object ISOSingleYear {

  def baseFormats = Array("uuuu")
  
  def baseParseOffsets = false
  
  def formats = baseFormats
  
  def parseOffsets = baseParseOffsets

  def parsedFormats = formats.map(f => DateParser.newDateFormat(f, false, true, true, parseOffsets))
  
  /**
   * Extraction method
   */
  def unapply(str: String): Option[EventDate] = {
    try {
      val eventDateParsed: Option[LocalDate] = DateParser.parseByFormat(str, parsedFormats)
      
      if (eventDateParsed.isDefined) {
        val eventDateSerialised = eventDateParsed.get.format(DateParser.YEAR)
        val startDate = eventDateSerialised
        val endDate = eventDateSerialised
        val startYear, endYear = eventDateParsed.get.format(DateParser.YEAR)
        val startMonth, endMonth = ""
        val startDay, endDay = ""
        val eventDateLegacyClass = DateParser.fromLocalDate(eventDateParsed).get

        Some(EventDate(eventDateLegacyClass, startDate, startDay, startMonth, startYear, eventDateLegacyClass, endDate, endDay,
          endMonth: String, endYear, false))
      } else {
        None
      }
    } catch {
      case e: Exception => None
    }
  }
}

/** Extractor for the format uuuu-MM-dd */
class SingleDate {

  def baseFormats = Array("uuuu-MM-dd","uuuu/MM/dd")

//  2001-03-14T00:00:00+11:00
  def formats = baseFormats.map(f => Array(f, f + "'Z'", f + "'T'hh:mm'Z'",f + "'T'HH:mm'Z'", f + "'T'hh:mm:ss",f + "'T'HH:mm:ss", f + "'T'hh:mm:ss'Z'", f + "'T'HH:mm:ss'Z'",f + " hh:mm:ss",f + " HH:mm:ss")).flatten

//  def parsedFormats = formats.map(f => DateParser.newDateFormat(f))
  def parsedFormats = formats.map(f => DateParser.newDateFormat(f)) ++ Array(DateParser.OFFSET_DATE_OPTIONAL_TIME, DateParser.YEAR_MONTH_TO_LOCAL_DATE, DateParser.YEAR_TO_LOCAL_DATE, DateParser.NON_ISO_OFFSET_DATE_OPTIONAL_TIME)

  /**
   * Extraction method
   */
  def unapply(str: String): Option[EventDate] = {
    try {
      // Parse everything down to a LocalDate if possible
      // Checks the ISO date formats before checking the custom formats
      val eventDateParsed: Option[LocalDate] = DateParser.parseISOOrFormats(str, parsedFormats)
      
      if (eventDateParsed.isDefined) {
        val eventDateSerialised = eventDateParsed.get.format(DateTimeFormatter.ISO_LOCAL_DATE)
        val startDate = eventDateSerialised
        val endDate = eventDateSerialised
        val startYear, endYear = eventDateParsed.get.format(DateParser.YEAR)
        val startMonth, endMonth = eventDateParsed.get.format(DateParser.MONTH)
        val startDay, endDay = eventDateParsed.get.format(DateParser.DAY)
        val eventDateLegacyClass = DateParser.fromLocalDate(eventDateParsed).get

        Some(EventDate(eventDateLegacyClass, startDate, startDay, startMonth, startYear, eventDateLegacyClass, endDate, endDay,
          endMonth: String, endYear, true))
      } else {
        None
      }
    } catch {
      case e: Exception => None
    }
  }
}

trait NonISOSingleDate extends SingleDate {
  override def baseFormats = Array("dd-MM-uuuu","dd/MM/uuuu","dd-MMM-uuuu","dd/MMM/uuuu","dd MMM uuuu")
}

trait NonISODateRange extends DateRange {
  override def baseFormats = Array("dd-MM-uuuu","dd/MM/uuuu","dd-MMM-uuuu","dd/MMM/uuuu","dd MMM uuuu")
}

object NonISOTruncatedYearDate {
  def baseFormats = Array(("dd-MM-", ""), ("dd/MM/", ""))

//  2001-03-14T00:00:00+11:00
  def formats = baseFormats.map(f => Array(f, (f._1, f._2 + "'Z'"), (f._1, f._2 + "'T'hh:mm'Z'"), (f._1, f._2 + "'T'HH:mm'Z'"), (f._1, f._2 + "'T'hh:mm:ss"), (f._1, f._2 + "'T'HH:mm:ss"), (f._1, f._2 + "'T'hh:mm:ss'Z'"), (f._1, f._2 + "'T'HH:mm:ss'Z'"), (f._1, f._2 + " hh:mm:ss"), (f._1, f._2 + " HH:mm:ss"))).flatten

  def parsedFormats = formats.map(f => DateParser.newTwoDigitYearDateFormat(f._1, f._2))
  
  /**
   * Extraction method
   */
  def unapply(str: String): Option[EventDate] = {
    try {
      // Parse everything down to a LocalDate if possible
      // Checks the ISO date formats before checking the custom formats
      val eventDateParsed: Option[LocalDate] = DateParser.parseISOOrFormats(str, parsedFormats)
      
      if (eventDateParsed.isDefined) {
        val eventDateSerialised = eventDateParsed.get.format(DateTimeFormatter.ISO_LOCAL_DATE)
        val startDate = eventDateSerialised
        val endDate = eventDateSerialised
        val startYear, endYear = eventDateParsed.get.format(DateParser.YEAR)
        val startMonth, endMonth = eventDateParsed.get.format(DateParser.MONTH)
        val startDay, endDay = eventDateParsed.get.format(DateParser.DAY)
        val eventDateLegacyClass = DateParser.fromLocalDate(eventDateParsed).get

        Some(EventDate(eventDateLegacyClass, startDate, startDay, startMonth, startYear, eventDateLegacyClass, endDate, endDay,
          endMonth: String, endYear, true))
      } else {
        None
      }
    } catch {
      case e: Exception => None
    }
  }

}

object ISOSingleDate extends SingleDate

object NonISOSingleDate extends NonISOSingleDate

object NonISODateRange extends NonISODateRange

/** Extractor for the format uuuu-MM */
object ISOMonthDate {

  def baseFormats = Array("uuuu-MM", "uuuu-MM-", "MM uuuu", "MMM-uuuu", "uuuu-MM-00")
  
  def formats = baseFormats
  
  def parsedFormats = formats.map(f => DateParser.newDateFormat(f, false, false, true))

  /**
   * Extraction method
   */
  def unapply(str: String): Option[EventDate] = {
    try {
      val eventDateParsed: Option[LocalDate] = DateParser.parseByFormat(str, parsedFormats)
      
      if (eventDateParsed.isDefined) {
        val eventDateSerialised = eventDateParsed.get.format(DateParser.YEAR_MONTH)
        val startDate = eventDateSerialised
        val endDate = eventDateSerialised
        val startYear, endYear = eventDateParsed.get.format(DateParser.YEAR)
        val startMonth, endMonth = eventDateParsed.get.format(DateParser.MONTH)
        // Ignore day as it is defaulted in all of the patterns for this parser
        val startDay, endDay = ""
        val eventDateLegacyClass = DateParser.fromLocalDate(eventDateParsed).get

        // Note: The singleDate flag is not used as expected here, it must be true to fit the current logic in EventProcessor.reformatToPrecision
        Some(EventDate(eventDateLegacyClass, startDate, startDay, startMonth, startYear, eventDateLegacyClass, endDate, endDay,
          endMonth: String, endYear, true))
      } else {
        None
      }
    } catch {
      case e: Exception => None
    }
  }
}

object ISODateRange extends DateRange

/** Extractor for the format uuuu-MM-dd/uuuu-MM-dd */
class DateRange {

  def baseFormats = Array("uuuu-MM-dd", "uuuu-M-d", "uuuu-MM-dd'T'hh:mm-ss", "uuuu-MM-dd'T'HH:mm-ss", "uuuu-MM-dd'T'hh:mm'Z'", "uuuu-MM-dd'T'HH:mm'Z'")

  def formats = baseFormats

  def parsedFormats = formats.map(f => DateParser.newDateFormat(f)) ++ Array(DateParser.OFFSET_DATE_OPTIONAL_TIME, DateParser.NON_ISO_OFFSET_DATE_OPTIONAL_TIME)

  def unapply(str: String): Option[EventDate] = {
    try {

      val parts = ParseUtil.splitRange(str)
      if (parts.length != 2) return None
      val startDateParsed: Option[LocalDate] = DateParser.parseISOOrFormats(parts(0), parsedFormats)
      val endDateParsed: Option[LocalDate] = DateParser.parseISOOrFormats(parts(1), parsedFormats)

      if (startDateParsed.isDefined) {
        val startDateSerialised = startDateParsed.get.format(DateTimeFormatter.ISO_LOCAL_DATE)
        val startDate = startDateSerialised
        val startYear = startDateParsed.get.format(DateParser.YEAR)
        val startMonth = startDateParsed.get.format(DateParser.MONTH)
        val startDay = startDateParsed.get.format(DateParser.DAY)
        val startEventDateLegacyClass = DateParser.fromLocalDate(startDateParsed).get
        var endDate = ""
        var endYear = startYear
        var endMonth = startMonth
        var endDay = startDay
        if(endDateParsed.isDefined) {
          endYear = endDateParsed.get.format(DateParser.YEAR)
          endMonth = endDateParsed.get.format(DateParser.MONTH)
          endDay = endDateParsed.get.format(DateParser.DAY)
          endDate = LocalDate.of(Integer.parseInt(endYear), Integer.parseInt(endMonth), Integer.parseInt(endDay)).format(DateTimeFormatter.ISO_LOCAL_DATE)
          val endEventDateLegacyClass = DateParser.fromLocalDate(endDateParsed).get
          
          Some(EventDate(startEventDateLegacyClass, startDate, startDay, startMonth, startYear, endEventDateLegacyClass, endDate, endDay,
            endMonth: String, endYear, startDate.equals(endDate)))
        } else {
          None
        }
      } else {
        None
      }
    } catch {
      case e: Exception => {
        DateParser.logger.warn("Could not parse date range: ", e)
        DateParser.logger.warn("formats={}", formats)
        None
      }
    }
  }
}

/** Extractor for the format uuuu-MM/uuuu-MM */
object ISOMonthYearDateRange {
  def unapply(str: String): Option[EventDate] = {
    try {

      val parts = ParseUtil.splitRange(str)
      if (parts.length != 2) return None
      val startDateParsed: Option[LocalDate] = DateParser.parseISOOrFormats(parts(0), Array(DateParser.YEAR_MONTH_TO_LOCAL_DATE))

      if (startDateParsed.isDefined) {
        val startDateSerialised = startDateParsed.get.format(DateTimeFormatter.ISO_LOCAL_DATE)
        val startDate = startDateSerialised
        val startYear = startDateParsed.get.format(DateParser.YEAR)
        val startMonth = startDateParsed.get.format(DateParser.MONTH)
        val startDay = startDateParsed.get.format(DateParser.DAY)
        val startEventDateLegacyClass = DateParser.fromLocalDate(startDateParsed).get
        val endDateParsed = Some(LocalDate.parse(parts(1), DateParser.YEAR_MONTH_TO_LOCAL_DATE))
        if (endDateParsed.isDefined) {
          var endYear = endDateParsed.get.format(DateParser.YEAR)
          var endMonth = endDateParsed.get.format(DateParser.MONTH)
          var endDay = endDateParsed.get.format(DateParser.DAY)
          var endDate = Some(LocalDate.of(Integer.parseInt(endYear), Integer.parseInt(endMonth), 1))
          val endDateSerialised = endDate.get.format(DateTimeFormatter.ISO_LOCAL_DATE)
          val endEventDateLegacyClass = DateParser.fromLocalDate(endDate).get
          
          Some(EventDate(startEventDateLegacyClass, startDate, startDay, startMonth, startYear, endEventDateLegacyClass, endDateSerialised, endDay,
            endMonth: String, endYear, startDate.equals(endDate)))
        } else {
          None
        }
      } else {
        None
      }
    } catch {
      case e: Exception => {
        DateParser.logger.warn("Could not parse ISOMonthDateRange: ", e)
        None
      }
    }
  }
}

/** Extractor for the format uuuu-MM/MM */
object ISOMonthDateRange {
  def unapply(str: String): Option[EventDate] = {
    try {

      val parts = ParseUtil.splitRange(str)
      if (parts.length != 2) return None
      val startDateParsed: Option[LocalDate] = DateParser.parseISOOrFormats(parts(0), Array(DateParser.YEAR_MONTH_TO_LOCAL_DATE))

      if (startDateParsed.isDefined) {
        val startDateSerialised = startDateParsed.get.format(DateTimeFormatter.ISO_LOCAL_DATE)
        val startDate = startDateSerialised
        val startYear = startDateParsed.get.format(DateParser.YEAR)
        val startMonth = startDateParsed.get.format(DateParser.MONTH)
        val startDay = startDateParsed.get.format(DateParser.DAY)
        val startEventDateLegacyClass = DateParser.fromLocalDate(startDateParsed).get
        val endDateParsed = Some(LocalDate.parse(parts(1), DateParser.MONTH_TO_LOCAL_DATE))
        if (endDateParsed.isDefined) {
          var endYear = startYear
          var endMonth = endDateParsed.get.format(DateParser.MONTH)
          var endDay = endDateParsed.get.format(DateParser.DAY)
          var endDate = Some(LocalDate.of(Integer.parseInt(endYear), Integer.parseInt(endMonth), 1))
          val endDateSerialised = endDate.get.format(DateTimeFormatter.ISO_LOCAL_DATE)
          val endEventDateLegacyClass = DateParser.fromLocalDate(endDate).get
          
          Some(EventDate(startEventDateLegacyClass, startDate, startDay, startMonth, startYear, endEventDateLegacyClass, endDateSerialised, endDay,
            endMonth: String, endYear, startDate.equals(endDate)))
        } else {
          None
        }
      } else {
        None
      }
    } catch {
      case e: Exception => {
        DateParser.logger.warn("Could not parse ISOMonthDateRange: ", e)
        None
      }
    }
  }
}

/** Extractor for a full date time duration, using both ISO 'T' and a ' ' separated version */
object ISODateTimeRange {

  def unapply(str: String): Option[EventDate] = {
    try {

      val parts = ParseUtil.splitRange(str)
      if (parts.length != 2) return None
      val startDateParsed: Option[LocalDate] = DateParser.parseISOOrFormats(parts(0), Array(DateParser.OFFSET_DATE_OPTIONAL_TIME, DateParser.NON_ISO_OFFSET_DATE_OPTIONAL_TIME))
      val endDateParsed: Option[LocalDate] = DateParser.parseISOOrFormats(parts(1), Array(DateParser.OFFSET_DATE_OPTIONAL_TIME, DateParser.NON_ISO_OFFSET_DATE_OPTIONAL_TIME))

      if (startDateParsed.isDefined) {
        val startDateSerialised = startDateParsed.get.format(DateTimeFormatter.ISO_LOCAL_DATE)
        val startDate = startDateSerialised
        val startYear = startDateParsed.get.format(DateParser.YEAR)
        val startMonth = startDateParsed.get.format(DateParser.MONTH)
        val startDay = startDateParsed.get.format(DateParser.DAY)
        val startEventDateLegacyClass = DateParser.fromLocalDate(startDateParsed).get
        if (endDateParsed.isDefined) {
          var endYear = endDateParsed.get.format(DateParser.YEAR)
          var endMonth = endDateParsed.get.format(DateParser.MONTH)
          var endDay = endDateParsed.get.format(DateParser.DAY)
          val endDateSerialised = endDateParsed.get.format(DateTimeFormatter.ISO_LOCAL_DATE)
          val endEventDateLegacyClass = DateParser.fromLocalDate(endDateParsed).get
          
          Some(EventDate(startEventDateLegacyClass, startDate, startDay, startMonth, startYear, endEventDateLegacyClass, endDateSerialised, endDay,
            endMonth: String, endYear, startDateSerialised.equals(endDateSerialised)))
        } else {
          None
        }
      } else {
        None
      }
    } catch {
      case e: Exception => {
        DateParser.logger.warn("Could not parse ISOMonthDateRange: ", e)
        None
      }
    }
  }
}

/** Extractor for the format Fri Aug 12 15:19:20 EST 2011 */
object ISOVerboseDateTime {

  def baseFormats = Array("EEE MMM dd hh:mm:ss zzz uuuu","EEE MMM dd HH:mm:ss zzz uuuu")
  
  def formats = baseFormats
  
  def parsedFormats = formats.map(f => DateParser.newDateFormat(f))

  /**
   * Extraction method
   */
  def unapply(str: String): Option[EventDate] = {
    try {
      val eventDateParsed: Option[LocalDate] = DateParser.parseISOOrFormats(str, parsedFormats)
      
      if (eventDateParsed.isDefined) {
        val eventDateSerialised = eventDateParsed.get.format(DateTimeFormatter.ISO_LOCAL_DATE)
        val startDate = eventDateSerialised
        val endDate = eventDateSerialised
        val startYear, endYear = eventDateParsed.get.format(DateParser.YEAR)
        val startMonth, endMonth = eventDateParsed.get.format(DateParser.MONTH)
        val startDay, endDay = eventDateParsed.get.format(DateParser.DAY)
        val eventDateLegacyClass = DateParser.fromLocalDate(eventDateParsed).get

        Some(EventDate(eventDateLegacyClass, startDate, startDay, startMonth, startYear, eventDateLegacyClass, endDate, endDay,
          endMonth: String, endYear, true))
      } else {
        None
      }
    } catch {
      case e: Exception => None
    }
  }
}

/** Extractor for the format Mon Apr 23 00:00:00 EST 1984/Sun Apr 29 00:00:00 EST 1984 */
object ISOVerboseDateTimeRange {

  def baseFormats = Array("EEE MMM dd hh:mm:ss zzz uuuu","EEE MMM dd HH:mm:ss zzz uuuu")

  def formats = baseFormats
  
  def parsedFormats = formats.map(f => DateParser.newDateFormat(f))

  def unapply(str: String): Option[EventDate] = {
    try {
      val parts = ParseUtil.splitRange(str)
      if (parts.length != 2) return None
      val startDateParsed: Option[LocalDate] = DateParser.parseISOOrFormats(parts(0), parsedFormats)
      val endDateParsed: Option[LocalDate] = DateParser.parseISOOrFormats(parts(1), parsedFormats)

      if (startDateParsed.isDefined) {
        val startDateSerialised = startDateParsed.get.format(DateTimeFormatter.ISO_LOCAL_DATE)
        val startDate = startDateSerialised
        val startYear = startDateParsed.get.format(DateParser.YEAR)
        val startMonth = startDateParsed.get.format(DateParser.MONTH)
        val startDay = startDateParsed.get.format(DateParser.DAY)
        val startEventDateLegacyClass = DateParser.fromLocalDate(startDateParsed).get
        var endDate = ""
        var endYear = ""
        var endMonth = ""
        var endDay = ""
        if(endDateParsed.isDefined) {
          val endDateSerialised = endDateParsed.get.format(DateTimeFormatter.ISO_LOCAL_DATE)
          endDate = endDateSerialised
          endYear = endDateParsed.get.format(DateParser.YEAR)
          endMonth = endDateParsed.get.format(DateParser.MONTH)
          endDay = endDateParsed.get.format(DateParser.DAY)
          val endEventDateLegacyClass = DateParser.fromLocalDate(endDateParsed).get
          
          Some(EventDate(startEventDateLegacyClass, startDate, startDay, startMonth, startYear, endEventDateLegacyClass, endDate, endDay,
            endMonth: String, endYear, startDate.equals(endDate)))
        } else {
          None
        }
      } else {
        None
      }
    } catch {
      case e: Exception => None
    }
  }
}


/** Extractor for the format uuuu-MM-dd/MM-dd */
object ISODayMonthRange {
  def baseFormats = Array("uuuu-MM-dd", "uuuu-M-d")

  def formats = baseFormats
  
  def parsedFormats = formats.map(f => DateParser.newDateFormat(f))
  
  def unapply(str: String): Option[EventDate] = {
    try {

      val parts = ParseUtil.splitRange(str)
      if (parts.length != 2) return None
      val startDateParsed: Option[LocalDate] = DateParser.parseISOOrFormats(parts(0), parsedFormats)
      val endDateParsed: Option[LocalDate] = DateParser.parseISOOrFormats(parts(1), Array(DateParser.MONTH_DAY_TO_LOCAL_DATE, DateParser.SHORT_MONTH_HYPHEN_DAY))

      if (startDateParsed.isDefined) {
        val startDateSerialised = startDateParsed.get.format(DateTimeFormatter.ISO_LOCAL_DATE)
        val startDate = startDateSerialised
        val startYear = startDateParsed.get.format(DateParser.YEAR)
        val startMonth = startDateParsed.get.format(DateParser.MONTH)
        val startDay = startDateParsed.get.format(DateParser.DAY)
        val startEventDateLegacyClass = DateParser.fromLocalDate(startDateParsed).get
        var endDate = ""
        var endYear = startYear
        var endMonth = ""
        var endDay = ""
        if(endDateParsed.isDefined) {
          endMonth = endDateParsed.get.format(DateParser.MONTH)
          endDay = endDateParsed.get.format(DateParser.DAY)
          endDate = LocalDate.of(Integer.parseInt(endYear), Integer.parseInt(endMonth), Integer.parseInt(endDay)).format(DateTimeFormatter.ISO_LOCAL_DATE)
          val endEventDateLegacyClass = DateParser.fromLocalDate(endDateParsed).get
          
          Some(EventDate(startEventDateLegacyClass, startDate, startDay, startMonth, startYear, endEventDateLegacyClass, endDate, endDay,
            endMonth: String, endYear, startDate.equals(endDate)))
        } else {
          None
        }
      } else {
        None
      }
    } catch {
      case e: Exception => {
        DateParser.logger.warn("Could not parse date range: ", e)
        DateParser.logger.warn("formats={}", formats)
        None
      }
    }
  }
}

/** Extractor for the format uuuu-MM-dd/dd */
object ISODayDateRange {
  def baseFormats = Array("uuuu-MM-dd")

  def formats = baseFormats
  
  def parsedFormats = formats.map(f => DateParser.newDateFormat(f))
  
  def unapply(str: String): Option[EventDate] = {
    try {

      val parts = ParseUtil.splitRange(str)
      if (parts.length != 2) return None
      val startDateParsed: Option[LocalDate] = DateParser.parseISOOrFormats(parts(0), parsedFormats)
      val endDateParsed: Option[LocalDate] = DateParser.parseISOOrFormats(parts(1), Array(DateParser.DAY_TO_LOCAL_DATE))

      if (startDateParsed.isDefined) {
        val startDateSerialised = startDateParsed.get.format(DateTimeFormatter.ISO_LOCAL_DATE)
        val startDate = startDateSerialised
        val startYear = startDateParsed.get.format(DateParser.YEAR)
        val startMonth = startDateParsed.get.format(DateParser.MONTH)
        val startDay = startDateParsed.get.format(DateParser.DAY)
        val startEventDateLegacyClass = DateParser.fromLocalDate(startDateParsed).get
        var endDate = ""
        var endYear = startYear
        var endMonth = startMonth
        var endDay = ""
        if(endDateParsed.isDefined) {
          endDay = endDateParsed.get.format(DateParser.DAY)
          endDate = LocalDate.of(Integer.parseInt(endYear), Integer.parseInt(endMonth), Integer.parseInt(endDay)).format(DateTimeFormatter.ISO_LOCAL_DATE)
          val endEventDateLegacyClass = DateParser.fromLocalDate(endDateParsed).get
          
          Some(EventDate(startEventDateLegacyClass, startDate, startDay, startMonth, startYear, endEventDateLegacyClass, endDate, endDay,
            endMonth: String, endYear, startDate.equals(endDate)))
        } else {
          None
        }
      } else {
        None
      }
    } catch {
      case e: Exception => {
        DateParser.logger.warn("Could not parse date range: ", e)
        DateParser.logger.warn("formats={}", formats)
        None
      }
    }
  }
}

/** Extractor for the format uuuu/uuuu and uuuu/yy and uuuu/y */
object ISOYearRange {

  def baseFormats = Array("uuuu")

  def formats = baseFormats
  
  def parsedFormats = formats.map(f => DateParser.newDateFormat(f, false, true, true, false))
  
  def unapply(str: String): Option[EventDate] = {
    try {

      val parts = ParseUtil.splitRange(str)
      if (parts.length != 2) return None
      val startDateParsed: Option[LocalDate] = DateParser.parseISOOrFormats(parts(0), parsedFormats)
      // Additionally support two digit years for the end date
      val endDateParsed: Option[LocalDate] = DateParser.parseISOOrFormats(parts(1), parsedFormats ++ Array(DateParser.newTwoDigitYearDateFormat("", "", 1920, true, true, true)))

      if (startDateParsed.isDefined) {
        val startDateSerialised = startDateParsed.get.format(DateTimeFormatter.ISO_LOCAL_DATE)
        val startDate = startDateSerialised
        val startYear = startDateParsed.get.format(DateParser.YEAR)
        val startMonth = startDateParsed.get.format(DateParser.MONTH)
        val startDay = startDateParsed.get.format(DateParser.DAY)
        val startEventDateLegacyClass = DateParser.fromLocalDate(startDateParsed).get
        var endDate = ""
        var endYear = startYear
        var endMonth = startMonth
        var endDay = startDay
        if(endDateParsed.isDefined) {
          endYear = endDateParsed.get.format(DateParser.YEAR)
          endMonth = endDateParsed.get.format(DateParser.MONTH)
          endDay = endDateParsed.get.format(DateParser.DAY)
          endDate = LocalDate.of(Integer.parseInt(endYear), Integer.parseInt(endMonth), Integer.parseInt(endDay)).format(DateTimeFormatter.ISO_LOCAL_DATE)
          val endEventDateLegacyClass = DateParser.fromLocalDate(endDateParsed).get
          
          Some(EventDate(startEventDateLegacyClass, startDate, startDay, startMonth, startYear, endEventDateLegacyClass, endDate, endDay,
            endMonth: String, endYear, startDate.equals(endDate)))
        } else {
          None
        }
      } else {
        None
      }
    } catch {
      case e: Exception => {
        DateParser.logger.warn("Could not parse date range: ", e)
        DateParser.logger.warn("formats={}", formats)
        None
      }
    }
  }
}


object ParseUtil {
  /**
   * Split textual or ISO range separators, trim, and removing any trailing dashes '-'
   */
  def splitRange(str:String) = {
    if(str.contains("&")){
      str.split("&").map(_.trim).map(s => if (s.endsWith("-")) { s.substring(0, s.length() - 1) } else { s })
    } else if (str.contains("to")){
      // FIXME: This will match "October"
      str.split("to").map(_.trim).map(s => if (s.endsWith("-")) { s.substring(0, s.length() - 1) } else { s })
    } else {
      str.split("/").map(_.trim).map(s => if (s.endsWith("-")) { s.substring(0, s.length() - 1) } else { s })
    }
  }
}
