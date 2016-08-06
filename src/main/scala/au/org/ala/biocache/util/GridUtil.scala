package au.org.ala.biocache.util

import java.util

/**
  * Utilities for parsing OS and Irish grid references.
  */
object GridUtil {

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
  val IRISH_CRS = "EPSG:29902"
  val OSGB_CRS = "EPSG:27700"

  /**
    * Derive a value from the grid reference accuracy for coordinateUncertaintyInMeters.
    *
    * @param noOfNumericalDigits
    * @param noOfSecondaryAlphaChars
    * @return
    */
  def getCoordinateUncertaintyFromGridRef(noOfNumericalDigits:Int, noOfSecondaryAlphaChars:Int) : Option[Int] = {
    val accuracy = noOfNumericalDigits match {
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
    * Takes a grid reference and returns a map of grid references at different resolutions.
    *
    * @param gridRef
    * @return
    */
  def getGridRefAsResolutions(gridRef:String) : java.util.Map[String, String] = {

    val map = new util.HashMap[String, String]

    gridReferenceToEastingNorthing(gridRef) match {
      case Some((gridletters, easting, northing, precision, minE, minN, maxE, maxN, datum)) => {

        map.put("grid_ref_100000", gridletters)

          if (gridRef.length > 2) {
            val eastingAsStr = easting.toString
            val northingAsStr = northing.toString

            //add grid references for 10km, and 1km
            if (eastingAsStr.length() >= 3 && northingAsStr.length() >= 3) {
              map.put("grid_ref_10000", gridletters + eastingAsStr.substring(1, 2) + northingAsStr.substring(1, 2))
            }
            if (eastingAsStr.length() >= 4 && northingAsStr.length() >= 4) {
              val eastingWithin10km = eastingAsStr.substring(2, 3).toInt
              val northingWithin10km = northingAsStr.substring(2, 3).toInt
              val tetrad = tetradLetters((eastingWithin10km / 2) * 5 + (northingWithin10km /2))
              map.put("grid_ref_2000", gridletters + eastingAsStr.substring(1, 2) + northingAsStr.substring(1, 2) + tetrad)
              map.put("grid_ref_1000", gridletters + eastingAsStr.substring(1, 3) + northingAsStr.substring(1, 3))
            }
            if (eastingAsStr.length() >= 5 && northingAsStr.length() >= 5) {
              map.put("grid_ref_100", gridletters + eastingAsStr.substring(1, 4) + northingAsStr.substring(1, 4))
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
  def gridReferenceToEastingNorthing(gridRef:String): Option[(String, Int, Int, Option[Int], Int, Int, Int, Int, String)] = {
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
  def irishGridReferenceToEastingNorthing(gridRef:String): Option[(String, Int, Int, Option[Int], Int, Int, Int, Int, String)] = {

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

    Some((gridletters, e, n, Some(coordinateUncertaintyOrZero), e, n, e + coordinateUncertaintyOrZero, n + coordinateUncertaintyOrZero, IRISH_CRS))
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
  def osGridReferenceToEastingNorthing(gridRef:String): Option[(String, Int, Int, Option[Int], Int, Int, Int, Int, String)] = {

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

    Some((gridletters, e, n, coordinateUncertainty, e, n, e + coordinateUncertaintyOrZero, n + coordinateUncertaintyOrZero, OSGB_CRS))
  }
}
