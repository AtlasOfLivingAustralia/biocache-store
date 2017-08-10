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

    val meddledMap = collection.mutable.Map(map.toSeq: _*)

    if(meddledMap.contains("geodeticDatum")){
      //Easting/Northing can include a latitude / longitude
      val projection = map.getOrElse("geodeticDatum", "")
      if (projection != ""){
        if (!projection.equalsIgnoreCase("OSGB") && !projection.equalsIgnoreCase("OSGB36") && !projection.equalsIgnoreCase("27700")){
          meddledMap.put("decimalLatitude", map.getOrElse("northing", ""))
          meddledMap.put("decimalLongitude", map.getOrElse("easting", ""))
        }
      }
    }

    //ZeroAbundance=Y - map this to occurrenceStatus - present / absent
    if(map.contains("ZeroAbundance")){
      val rawValue = map.getOrElse("ZeroAbundance", "")
      val zeroAbundance = BooleanUtils.toBoolean(rawValue)
      val occurrenceStatus = if(zeroAbundance){
        "absent"
      } else {
        "present"
      }
      meddledMap.put("occurrenceStatus", occurrenceStatus)
    }

    meddledMap.toMap
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
