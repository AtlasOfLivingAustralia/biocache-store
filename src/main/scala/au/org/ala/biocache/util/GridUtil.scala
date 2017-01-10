package au.org.ala.biocache.util

import java.util

import au.org.ala.biocache.model.QualityAssertion
import au.org.ala.biocache.vocab.{AssertionStatus, AssertionCodes}
import au.org.ala.biocache.vocab.AssertionCodes._
import au.org.ala.biocache.vocab.AssertionStatus._
import org.apache.commons.lang.StringUtils
import org.geotools.referencing.CRS
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

/**
  * Utilities for parsing UK Ordnance survey British and Irish grid references.
  */
object GridUtil {

  import StringHelper._, AssertionCodes._, AssertionStatus._, JavaConversions._

  val logger = LoggerFactory.getLogger("GridUtil")

  //deal with the 2k OS grid ref separately
  val osGridRefNoEastingNorthing = ("""([A-Z]{2})""").r
  val osGridRefRegex1Number = """([A-Z]{2})\s*([0-9]+)$""".r
  val osGridRef2kRegex = """([A-Z]{2})\s*([0-9]+)\s*([0-9]+)\s*([A-Z]{1})""".r
  val osGridRefRegex = """([A-Z]{2})\s*([0-9]+)\s*([0-9]+)$""".r
  val osGridRefWithQuadRegex = """([A-Z]{2})\s*([0-9]+)\s*([0-9]+)\s*([NW|NE|SW|SE]{2})$""".r

  //deal with the 2k OS grid ref separately
  val irishGridletterscodes = Array('A','B','C','D','F','G','H','J','L','M','N','O','Q','R','S','T','V','W','X','Y')
  val irishGridlettersFlattened = irishGridletterscodes.mkString
  val irishGridRefNoEastingNorthing = ("""(I?[""" + irishGridlettersFlattened +"""]{1})""").r
  val irishGridRefRegex1Number = """(I?[A-Z]{1})\s*([0-9]+)$""".r
  val irishGridRef2kRegex = """(I?[A-Z]{1})\s*([0-9]+)\s*([0-9]+)\s*([A-Z]{1})""".r
  val irishGridRefRegex = """(I?[A-Z]{1})\s*([0-9]+)\s*([0-9]+)$""".r
  val irishGridRefWithQuadRegex = """(I?[A-Z]{1})\s*([0-9]+)\s*([0-9]+)\s*([NW|NE|SW|SE]{2})$""".r
  val tetradLetters = Array('A','B','C','D','E','F','G','H','I','J','K','L','M','N','P','Q','R','S','T','U','V','W','X','Y','Z')

  //CRS
  val IRISH_CRS = "EPSG:29902"
  val OSGB_CRS = "EPSG:27700"

  lazy val crsEpsgCodesMap = {
    var valuesMap = Map[String, String]()
    for (line <- scala.io.Source.fromURL(getClass.getResource("/crsEpsgCodes.txt"), "utf-8").getLines().toList) {
      val values = line.split('=')
      valuesMap += (values(0) -> values(1))
    }
    valuesMap
  }

  lazy val zoneEpsgCodesMap = {
    var valuesMap = Map[String, String]()
    for (line <- scala.io.Source.fromURL(getClass.getResource("/zoneEpsgCodes.txt"), "utf-8").getLines().toList) {
      val values = line.split('=')
      valuesMap += (values(0) -> values(1))
    }
    valuesMap
  }
  /**
    * Derive a value from the grid reference accuracy for coordinateUncertaintyInMeters.
    *
    * @param noOfNumericalDigits
    * @param noOfSecondaryAlphaChars
    * @return
    */
  def getCoordinateUncertaintyFromGridRef(noOfNumericalDigits:Int, noOfSecondaryAlphaChars:Int) : Option[Int] = {
    val accuracy = noOfNumericalDigits match {
      case 10 => 1
      case 8 => 10
      case 6 => 100
      case 4 => 1000
      case 2 => 10000
      case 0 => 100000
      case _ => return None
    }
    noOfSecondaryAlphaChars match {
      case 2 => Some(accuracy / 2)
      case 1 => Some(accuracy / 5)
      case _ => Some(accuracy)
    }
  }

  /**
   * Reduce the resolution of the supplied grid reference.
   *
   * @param gridReference
   * @param uncertaintyString
   * @return
   */
  def convertReferenceToResolution(gridReference:String, uncertaintyString:String) : Option[String] = {

    try {
      val gridRefs = getGridRefAsResolutions(gridReference)
      val uncertainty = uncertaintyString.toInt

      val gridRefSeq = Array(
        gridRefs.getOrElse("grid_ref_100000", ""),
        gridRefs.getOrElse("grid_ref_10000", ""),
        gridRefs.getOrElse("grid_ref_2000", ""),
        gridRefs.getOrElse("grid_ref_1000", ""),
        gridRefs.getOrElse("grid_ref_100", "")
      )

      val ref = {
        if (uncertainty > 10000) {
          getBestValue(gridRefSeq, 0)
        } else if (uncertainty <= 10000 && uncertainty > 2000) {
          getBestValue(gridRefSeq, 1)
        } else if (uncertainty <= 2000 && uncertainty > 1000) {
          getBestValue(gridRefSeq, 2)
        } else if (uncertainty <= 1000 && uncertainty > 100) {
          getBestValue(gridRefSeq, 3)
        } else if (uncertainty < 100) {
          getBestValue(gridRefSeq, 4)
        } else {
          ""
        }
      }
      if(ref != "")
        Some(ref)
      else
        None
    } catch {
      case e:Exception => {
        logger.error("Problem converting grid reference " + gridReference + " to lower resolution of " + uncertaintyString, e)
        None
      }
    }
  }


  def getBestValue(values: Seq[String], preferredIndex:Int): String ={

    var counter = preferredIndex
    while(counter>=0){
      if(values(counter) != ""){
        return values(counter)
      }
      counter = counter -1
    }
    ""
  }

  private def padWithZeros(ref:String, pad:Int) = ("0" *  (pad - ref.length)) + ref

  /**
    * Takes a grid reference and returns a map of grid references at different resolutions.
    * Map will look like:
    * grid_ref_100000 -> "NO"
    * grid_ref_10000 -> "NO11"
    * grid_ref_1000 -> "NO1212"
    * grid_ref_100 -> "NO123123"
    *
    * @param gridRef
    * @return
    */
  def getGridRefAsResolutions(gridRef:String) : java.util.Map[String, String] = {

    val map = new util.HashMap[String, String]

    gridReferenceToEastingNorthing(gridRef) match {
      case Some(gr) => {

        val gridSize = gr.coordinateUncertainty.getOrElse(-1)
        map.put("grid_ref_100000", gr.gridLetters)

        if (gridRef.length > 2) {

          val eastingAsStr = padWithZeros((gr.easting.toInt % 100000).toString, 5)
          val northingAsStr = padWithZeros((gr.northing.toInt % 100000).toString, 5)

          //add grid references for 10km, and 1km
          if (eastingAsStr.length() >= 2 && northingAsStr.length() >= 2) {
            map.put("grid_ref_10000", gr.gridLetters + eastingAsStr.substring(0, 1) + northingAsStr.substring(0, 1))
          }
          if (eastingAsStr.length() >= 3 && northingAsStr.length() >= 3) {
            val eastingWithin10km = eastingAsStr.substring(1, 2).toInt
            val northingWithin10km = northingAsStr.substring(1, 2).toInt
            val tetrad = tetradLetters((eastingWithin10km / 2) * 5 + (northingWithin10km /2))

            if(gridSize != -1 && gridSize <= 2000){
              map.put("grid_ref_2000", gr.gridLetters + eastingAsStr.substring(0, 1) + northingAsStr.substring(0, 1) + tetrad)
            }
            if(gridSize != -1 && gridSize <= 1000) {
              map.put("grid_ref_1000", gr.gridLetters + eastingAsStr.substring(0, 2) + northingAsStr.substring(0, 2))
            }
          }

          if (gridSize != -1 && gridSize <= 100 && eastingAsStr.length > 3) {
            map.put("grid_ref_100", gr.gridLetters + eastingAsStr.substring(0, 3) + northingAsStr.substring(0, 3))
          }
        }
      }
      case None => //do nothing
    }
    map
  }


  /**
   * Takes a grid reference (british or irish) and returns easting, northing, datum and precision.
   */
  def gridReferenceToEastingNorthing(gridRef:String): Option[GridRef] = {
    val result = osGridReferenceToEastingNorthing(gridRef)
    if(!result.isEmpty){
      result
    } else {
      irishGridReferenceToEastingNorthing(gridRef)
    }
  }

  /**
    * Convert an ordnance survey grid reference to northing, easting and coordinateUncertaintyInMeters.
    * This is a port of this javascript code:
    *
    * http://www.movable-type.co.uk/scripts/latlong-gridref.html
    *
    * with additional extensions to handle 2km grid references e.g. NM39A
    *
    * @param gridRef
    * @return easting, northing, coordinate uncertainty in meters, minEasting, minNorthing, maxEasting, maxNorthing, coordinate system
    *
    */
  def irishGridReferenceToEastingNorthing(gridRef:String): Option[GridRef] = {

    // validate & parse format
    val (gridletters:String, easting:String, northing:String, twoKRef:String, quadRef:String, coordinateUncertainty:Option[Int]) = gridRef.trim() match {
      case irishGridRefRegex1Number(gridletters, oneNumber) => {
        val gridDigits = oneNumber.toString
        val en = Array(gridDigits.substring(0, gridDigits.length / 2), gridDigits.substring(gridDigits.length / 2))
        val coordUncertainty = getCoordinateUncertaintyFromGridRef(gridDigits.length, 0)
        (gridletters, en(0), en(1), "", "", coordUncertainty)
      }
      case irishGridRefRegex(gridletters, easting, northing) => {
        (gridletters, easting, northing, "", "", getCoordinateUncertaintyFromGridRef(easting.length * 2, 0))
      }
      case irishGridRef2kRegex(gridletters, easting, northing, twoKRef) => {
        (gridletters, easting, northing, twoKRef, "", getCoordinateUncertaintyFromGridRef(easting.length * 2, 1))
      }
      case irishGridRefWithQuadRegex(gridletters, easting, northing, quadRef) => {
        (gridletters, easting, northing, "", quadRef, getCoordinateUncertaintyFromGridRef(easting.length * 2, 2))
      }
      case irishGridRefNoEastingNorthing(gridletters) => {
        (gridletters, "0", "0", "",  "", getCoordinateUncertaintyFromGridRef(0,0))
      }
      case _ => return None
    }

    val singleGridLetter = if(gridletters.length == 2) gridletters.charAt(1) else gridletters.charAt(0)

    val gridIdx = irishGridletterscodes.indexOf(singleGridLetter)

    // convert grid letters into 100km-square indexes from false origin (grid square SV):
    val e100km = (gridIdx % 4)
    val n100km = (4 - (gridIdx / 4))

    val easting10digit = (easting + "00000").substring(0, 5)
    val northing10digit = (northing + "00000").substring(0, 5)

    var e = (e100km.toString + easting10digit).toInt
    var n = (n100km.toString + northing10digit).toInt

    /** C & P from below **/

    //handle the non standard grid parts
    if(twoKRef != ""){

      val cellSize = {
        if (easting.length == 1) 2000
        else if (easting.length == 2) 200
        else if (easting.length == 3) 20
        else if (easting.length == 4) 2
        else 0
      }

      //Dealing with 5 character grid references = 2km grids
      //http://www.kmbrc.org.uk/recording/help/gridrefhelp.php?page=6
      twoKRef match {
        case it if (Character.codePointAt(twoKRef, 0) <= 'N') => {
          e = e + (((Character.codePointAt(twoKRef, 0) - 65) / 5) * cellSize)
          n = n + (((Character.codePointAt(twoKRef, 0) - 65) % 5) * cellSize)
        }
        case it if (Character.codePointAt(twoKRef, 0) >= 'P') => {
          e = e + (((Character.codePointAt(twoKRef, 0) - 66) / 5) * cellSize)
          n = n + (((Character.codePointAt(twoKRef, 0) - 66) % 5) * cellSize)
        }
        case _ => return None
      }
    } else if(quadRef != ""){

      val cellSize = {
        if (easting.length == 1) 5000
        else if (easting.length == 2) 500
        else if (easting.length == 3) 50
        else if (easting.length == 4) 5
        else 0
      }
      if(cellSize > 0) {
        twoKRef match {
          case "NW" => {
            e = e + (cellSize / 2)
            n = n + (cellSize + cellSize / 2)
          }
          case "NE" => {
            e = e + (cellSize + cellSize / 2)
            n = n + (cellSize + cellSize / 2)
          }
          case "SW" => {
            e = e + (cellSize / 2)
            n = n + (cellSize / 2)
          }
          case "SE" => {
            e = e + (cellSize + cellSize / 2)
            n = n + (cellSize / 2)
          }
          case _ => return None
        }
      }
    }

    /** end of C & P ***/
    val coordinateUncertaintyOrZero = if(coordinateUncertainty.isEmpty) 0 else coordinateUncertainty.get

    Some(GridRef(gridletters, e, n, Some(coordinateUncertaintyOrZero), e, n, e + coordinateUncertaintyOrZero, n + coordinateUncertaintyOrZero, IRISH_CRS))
  }

  /**
    * Convert an ordnance survey grid reference to northing, easting and coordinateUncertaintyInMeters.
    * This is a port of this javascript code:
    *
    * http://www.movable-type.co.uk/scripts/latlong-gridref.html
    *
    * with additional extensions to handle 2km grid references e.g. NM39A
    *
    * @param gridRef
    * @return easting, northing, coordinate uncertainty in meters, minEasting, minNorthing, maxEasting, maxNorthing
    */
  def osGridReferenceToEastingNorthing(gridRef:String): Option[GridRef] = {

    //deal with the 2k OS grid ref separately
    val osGridRefNoEastingNorthing = ("""([A-Z]{2})""").r
    val osGridRefRegex1Number = """([A-Z]{2})\s*([0-9]+)$""".r
    val osGridRef2kRegex = """([A-Z]{2})\s*([0-9]+)\s*([0-9]+)\s*([A-Z]{1})""".r
    val osGridRefRegex = """([A-Z]{2})\s*([0-9]+)\s*([0-9]+)$""".r
    val osGridRefWithQuadRegex = """([A-Z]{2})\s*([0-9]+)\s*([0-9]+)\s*([NW|NE|SW|SE]{2})$""".r

    // validate & parse format
    val (gridletters:String, easting:String, northing:String, twoKRef:String, quadRef:String, coordinateUncertainty:Option[Int]) = gridRef.trim() match {
      case osGridRefRegex1Number(gridletters, oneNumber) => {
        val gridDigits = oneNumber.toString
        val en = Array(gridDigits.substring(0, gridDigits.length / 2), gridDigits.substring(gridDigits.length / 2))
        val coordUncertainty = getCoordinateUncertaintyFromGridRef(gridDigits.length, 0)
        (gridletters, en(0), en(1), "", "", coordUncertainty)
      }
      case osGridRefRegex(gridletters, easting, northing) => {
        (gridletters, easting, northing, "", "", getCoordinateUncertaintyFromGridRef(easting.length * 2, 0))
      }
      case osGridRef2kRegex(gridletters, easting, northing, twoKRef) => {
        (gridletters, easting, northing, twoKRef, "", getCoordinateUncertaintyFromGridRef(easting.length * 2, 1))
      }
      case osGridRefWithQuadRegex(gridletters, easting, northing, quadRef) => {
        (gridletters, easting, northing, "", quadRef, getCoordinateUncertaintyFromGridRef(easting.length * 2, 2))
      }
      case osGridRefNoEastingNorthing(gridletters) => {
        (gridletters, "0", "0", "",  "", getCoordinateUncertaintyFromGridRef(0,0))
      }
      case _ => return None
    }

    // get numeric values of letter references, mapping A->0, B->1, C->2, etc:
    val l1 = {
      val value = Character.codePointAt(gridletters, 0) - Character.codePointAt("A", 0)
      if(value > 7){
        value - 1
      } else {
        value
      }
    }
    val l2 = {
      val value = Character.codePointAt(gridletters, 1) - Character.codePointAt("A", 0)
      if(value > 7){
        value - 1
      } else {
        value
      }
    }

    // convert grid letters into 100km-square indexes from false origin (grid square SV):
    val e100km = (((l1-2) % 5) * 5 + (l2 % 5)).toInt
    val n100km = ((19 - Math.floor(l1 / 5) * 5) - Math.floor(l2 / 5)).toInt

    // validation
    if (e100km<0 || e100km>6 || n100km<0 || n100km>12) {
      return None
    }
    if (easting == null || northing == null){
      return None
    }
    if (easting.length() != northing.length()){
      return None
    }

    // standardise to 10-digit refs (metres)
    val easting10digit = (easting + "00000").substring(0, 5)
    val northing10digit = (northing + "00000").substring(0, 5)

    var e = (e100km.toString + easting10digit).toInt
    var n = (n100km.toString + northing10digit).toInt

    //handle the non standard grid parts
    if(twoKRef != ""){

      val cellSize = {
        if (easting.length == 1) 2000
        else if (easting.length == 2) 200
        else if (easting.length == 3) 20
        else if (easting.length == 4) 2
        else 0
      }

      //Dealing with 5 character grid references = 2km grids
      //http://www.kmbrc.org.uk/recording/help/gridrefhelp.php?page=6
      twoKRef match {
        case it if (Character.codePointAt(twoKRef, 0) <= 'N') => {
          e = e + (((Character.codePointAt(twoKRef, 0) - 65) / 5) * cellSize)
          n = n + (((Character.codePointAt(twoKRef, 0) - 65) % 5) * cellSize)
        }
        case it if (Character.codePointAt(twoKRef, 0) >= 'P') => {
          e = e + (((Character.codePointAt(twoKRef, 0) - 66) / 5) * cellSize)
          n = n + (((Character.codePointAt(twoKRef, 0) - 66) % 5) * cellSize)
        }
        case _ => return None
      }
    } else if(quadRef != ""){

      val cellSize = {
        if (easting.length == 1) 5000
        else if (easting.length == 2) 500
        else if (easting.length == 3) 50
        else if (easting.length == 4) 5
        else 0
      }
      if(cellSize > 0) {
        twoKRef match {
          case "NW" => {
            e = e + (cellSize / 2)
            n = n + (cellSize + cellSize / 2)
          }
          case "NE" => {
            e = e + (cellSize + cellSize / 2)
            n = n + (cellSize + cellSize / 2)
          }
          case "SW" => {
            e = e + (cellSize / 2)
            n = n + (cellSize / 2)
          }
          case "SE" => {
            e = e + (cellSize + cellSize / 2)
            n = n + (cellSize / 2)
          }
          case _ => return None
        }
      }
    }

    val coordinateUncertaintyOrZero = if(coordinateUncertainty.isEmpty) 0 else coordinateUncertainty.get

    Some(GridRef(gridletters, e, n, coordinateUncertainty, e, n, e + coordinateUncertaintyOrZero, n + coordinateUncertaintyOrZero, OSGB_CRS))
  }

  /**
    * Process supplied grid references. This currently only recognises UK OS grid references but could be
    * extended to support other systems.
    *
    * @param gridReference
    */
  def processGridReference(gridReference:String): Option[GISPoint] = {

    GridUtil.gridReferenceToEastingNorthing(gridReference) match {
      case Some(gr) => {

        //move coordinates to the centroid of the grid
        val reposition = if(!gr.coordinateUncertainty.isEmpty && gr.coordinateUncertainty.get > 0){
          gr.coordinateUncertainty.get / 2
        } else {
          0
        }

        val coords = GISUtil.reprojectCoordinatesToWGS84(gr.easting + reposition, gr.northing + reposition, gr.datum, 5)

        //reproject min/max lat/lng
        val bbox = Array(
          GISUtil.reprojectCoordinatesToWGS84(gr.minEasting, gr.minNorthing, gr.datum, 5),
          GISUtil.reprojectCoordinatesToWGS84(gr.maxEasting, gr.maxNorthing, gr.datum, 5)
        )

        if(!coords.isEmpty){
          val (latitude, longitude) = coords.get
          val uncertaintyToUse = if(!gr.coordinateUncertainty.isEmpty){
            gr.coordinateUncertainty.get.toString
          } else {
            null
          }
          Some(GISPoint(
            latitude,
            longitude,
            GISUtil.WGS84_EPSG_Code,
            uncertaintyToUse,
            easting = gr.easting.toString,
            northing = gr.northing.toString,
            minLatitude = bbox(0).get._1,
            minLongitude = bbox(0).get._2,
            maxLatitude = bbox(1).get._1,
            maxLongitude = bbox(1).get._2
          ))
        } else {
          None
        }
      }
      case None => None
    }
  }

  /**
    * Get the EPSG code associated with a coordinate reference system string e.g. "WGS84" or "AGD66".
    *
    * @param crs The coordinate reference system string.
    * @return The EPSG code associated with the CRS, or None if no matching code could be found.
    *         If the supplied string is already a valid EPSG code, it will simply be returned.
    */
   def lookupEpsgCode(crs: String): Option[String] = {
    if (StringUtils.startsWithIgnoreCase(crs, "EPSG:")) {
      // Do a lookup with the EPSG code to ensure that it is valid
      try {
        CRS.decode(crs.toUpperCase)
        // lookup was successful so just return the EPSG code
        Some(crs.toUpperCase)
      } catch {
        case ex: Exception => None
      }
    } else if (crsEpsgCodesMap.contains(crs.toUpperCase)) {
      Some(crsEpsgCodesMap(crs.toUpperCase()))
    } else {
      None
    }
  }

  /**
    * Converts a easting northing to a decimal latitude/longitude.
    *
    * @param verbatimSRS
    * @param easting
    * @param northing
    * @param zone
    * @param assertions
    * @return 3-tuple reprojectedLatitude, reprojectedLongitude, WGS84_EPSG_Code
    */
  def processNorthingEastingZone(verbatimSRS: String, easting: String, northing: String, zone: String,
                                         assertions: ArrayBuffer[QualityAssertion]): Option[GISPoint] = {

    // Need a datum and a zone to get an epsg code for transforming easting/northing values
    val epsgCodeKey = {
      if (verbatimSRS != null) {
        verbatimSRS.toUpperCase + "|" + zone
      } else {
        // Assume GDA94 / MGA zone
        "GDA94|" + zone
      }
    }

    if (zoneEpsgCodesMap.contains(epsgCodeKey)) {
      val crsEpsgCode = zoneEpsgCodesMap(epsgCodeKey)
      val eastingAsDouble = easting.toDoubleWithOption
      val northingAsDouble = northing.toDoubleWithOption

      if (!eastingAsDouble.isEmpty && !northingAsDouble.isEmpty) {
        // Always round to 5 decimal places as easting/northing values are in metres and 0.00001 degree is approximately equal to 1m.
        val reprojectedCoords = GISUtil.reprojectCoordinatesToWGS84(eastingAsDouble.get, northingAsDouble.get, crsEpsgCode, 5)
        if (reprojectedCoords.isEmpty) {
          assertions += QualityAssertion(DECIMAL_LAT_LONG_CALCULATION_FROM_EASTING_NORTHING_FAILED,
            "Transformation of verbatim easting and northing to WGS84 failed")
          None
        } else {
          //lat and long from easting and northing did NOT fail:
          assertions += QualityAssertion(DECIMAL_LAT_LONG_CALCULATION_FROM_EASTING_NORTHING_FAILED, PASSED)
          assertions += QualityAssertion(DECIMAL_LAT_LONG_CALCULATED_FROM_EASTING_NORTHING,
            "Decimal latitude and longitude were calculated using easting, northing and zone.")
          val (reprojectedLatitude, reprojectedLongitude) = reprojectedCoords.get
          Some(GISPoint(reprojectedLatitude, reprojectedLongitude, GISUtil.WGS84_EPSG_Code, null))
        }
      } else {
        None
      }
    } else {
      if (verbatimSRS == null) {
        assertions += QualityAssertion(DECIMAL_LAT_LONG_CALCULATION_FROM_EASTING_NORTHING_FAILED,
          "Unrecognized zone GDA94 / MGA zone " + zone)
      } else {
        assertions += QualityAssertion(DECIMAL_LAT_LONG_CALCULATION_FROM_EASTING_NORTHING_FAILED,
          "Unrecognized zone " + verbatimSRS + " / zone " + zone)
      }
      None
    }
  }
}
