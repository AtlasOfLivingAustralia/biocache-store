package au.org.ala.biocache.util

import org.apache.commons.lang.time.{DateUtils, DateFormatUtils}
import java.util.Date
import au.org.ala.biocache.parser.EventDate
import au.org.ala.biocache.parser.DateParser
import java.time.format.DateTimeFormatter
import java.time.LocalDate

/**
 * Date util
 */
object DateUtil {

  def getCurrentYear = DateFormatUtils.format(new Date(), "yyyy").toInt

  def isFutureDate(date:EventDate) : Boolean = {
    val (str, formats):(String, Array[DateTimeFormatter]) ={
      date match{
        case dt if dt.startDate != "" => (dt.startDate, Array(DateTimeFormatter.ISO_LOCAL_DATE))
        case dt if dt.startYear!= "" && dt.startMonth != "" => (dt.startYear +"-" + dt.startMonth, Array(DateParser.YEAR_MONTH_TO_LOCAL_DATE))
        case dt if dt.startYear != "" => (dt.startYear, Array(DateParser.YEAR_TO_LOCAL_DATE))
        case _ => (null, Array())
      }
    }
    //check for future date
    if(str != null){
      val date = DateParser.parseByFormat(str, formats)
      date.isDefined && date.get.isAfter(LocalDate.now())
    } else {
      false
    }
  }
}
