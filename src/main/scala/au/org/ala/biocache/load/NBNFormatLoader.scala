package au.org.ala.biocache.load

import au.com.bytecode.opencsv.CSVReader
import org.apache.commons.lang3.BooleanUtils

/**
  * A CSV/TSV loader that performs some additional DwC mapping
  * required for the NBN format
  */
class NBNFormatLoader extends DwcCSVLoader {

  /**
   * Interprete some of the NBN format values into DwC terms and values
   *
   * @param map
   * @return
   */
  override def meddle(map: Map[String, String]): Map[String, String] = {

    //ZeroAbundance=Y - map this to occurrenceStatus - present / absent
    if(map.contains("ZeroAbundance")){
      val rawValue = map.getOrElse("ZeroAbundance", "")
      val zeroAbundance = BooleanUtils.toBoolean(rawValue)
      val occurrenceStatus = if(zeroAbundance){
        "absent"
      } else {
        "present"
      }
      map + ("occurrenceStatus" -> occurrenceStatus)
    } else {
      map
    }
  }

  /**
    *
    * @param reader
    * @return
    */
  override def mapHeadersToDwC(reader: CSVReader): Seq[String] = {
    val headers = super.mapHeadersToDwC(reader)
    headers
  }
}
