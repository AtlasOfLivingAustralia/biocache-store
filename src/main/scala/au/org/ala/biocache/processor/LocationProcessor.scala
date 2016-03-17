package au.org.ala.biocache.processor

import org.slf4j.LoggerFactory
import au.org.ala.biocache._
import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer
import org.apache.commons.lang.StringUtils
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.referencing.{CRS}
import org.geotools.referencing.operation.DefaultCoordinateOperationFactory
import org.geotools.geometry.GeneralDirectPosition
import org.apache.commons.math3.util.Precision
import au.org.ala.biocache.caches.{SpatialLayerDAO, TaxonProfileDAO, LocationDAO}
import au.org.ala.biocache.parser.{DistanceRangeParser, VerbatimLatLongParser}
import au.org.ala.biocache.model._
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.vocab._
import au.org.ala.biocache.util.{StringHelper}

/**
 * Processor of location information.
 */
class LocationProcessor extends Processor {

  import StringHelper._, AssertionCodes._, AssertionStatus._, JavaConversions._

  val logger = LoggerFactory.getLogger("LocationProcessor")

  val WGS84_EPSG_Code = "EPSG:4326"

  /**
   * Process geospatial details of the record. This step parses coordinates and cordinate precision values.
   * It performs a large number of tests on the supplied geospatial information.
   */
  def process(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {

    logger.debug(s"Processing location for guid: $guid")

    //retrieve the point
    val assertions = new ArrayBuffer[QualityAssertion]

    //handle the situation where the coordinates have already been sensitised
    setProcessedCoordinates(raw, processed, assertions)

    //parse altitude and depth values
    processAltitudeAndDepth(guid, raw, processed, assertions)

    //Continue processing location if a processed longitude and latitude exists
    if (processed.location.decimalLatitude != null && processed.location.decimalLongitude != null) {

      //store the point for downstream processing
      LocationDAO.storePointForSampling(processed.location.decimalLatitude, processed.location.decimalLongitude)

      //validate the coordinate values
      validateCoordinatesValues(raw, processed, assertions)

      //validate coordinate accuracy (coordinateUncertaintyInMeters) and coordinatePrecision (precision - A. Chapman)
      checkCoordinateUncertainty(raw, processed, assertions)

      //intersect values with sensitive areas
      val intersectValues = SpatialLayerDAO.intersect(processed.location.decimalLongitude, processed.location.decimalLatitude)

      //add state province, country, LGA
      processed.location.stateProvince = intersectValues.getOrElse(Config.stateProvinceLayerID, null)
      processed.location.lga  = intersectValues.getOrElse(Config.localGovLayerID, null)
      processed.location.country = intersectValues.getOrElse(Config.countriesLayerID, null)

      if (processed.location.country == null && processed.location.stateProvince != null) {
        processed.location.country = Config.defaultCountry
      }

      //habitat, no standard vocab available
      processed.location.habitat = raw.location.habitat

      //add the layers that are associated with the point
      processed.location.biome = {
        if (intersectValues.getOrElse(Config.terrestrialLayerID, null) != null) "Terrestrial"
        else if (intersectValues.getOrElse(Config.marineLayerID, null) != null) "Marine"
        else null
      }

      //check matched stateProvince
      checkForStateMismatch(raw, processed, assertions)

      //add the conservation status if necessary
      addConservationStatus(raw, processed)

      //check marine/non-marine
      checkForBiomeMismatch(raw, processed, assertions)
    }

    //create flag if no location info was supplied for this record
    checkLocationSupplied(raw, processed, assertions)

    //run validation tests against the processed coordinates
    validateCoordinates(raw, processed, assertions)

    //process state/country values if coordinates not determined
    processStateCountryValues(raw, processed, assertions)

    //validate the geo-reference values
    validateGeoreferenceValues(raw, processed, assertions)

    //return the assertions created by this processor
    assertions.toArray
  }

  /**
   * Create flag if no location info was supplied for this record
   *
   * @param raw
   * @param processed
   * @param assertions
   * @return
   */
  def checkLocationSupplied(raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]): ArrayBuffer[QualityAssertion] = {
    if (processed.location.decimalLatitude == null || processed.location.decimalLongitude == null) {
      //check to see if we have any location information at all for the record
      if (raw.location.footprintWKT == null && raw.location.locality == null && raw.location.locationID == null) {
        assertions += QualityAssertion(LOCATION_NOT_SUPPLIED)
      } else {
        assertions += QualityAssertion(LOCATION_NOT_SUPPLIED, PASSED)
      }
    } else {
      assertions += QualityAssertion(LOCATION_NOT_SUPPLIED, PASSED)
    }
  }

  /**
   * If no coordinates have been supplied, parse raw state and country values to vocabularies.
   *
   * @param raw
   * @param processed
   * @param assertions
   */
  private def processStateCountryValues(raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]){

    //Only process the raw state value if no latitude and longitude is provided
    if (processed.location.stateProvince == null && raw.location.decimalLatitude == null && raw.location.decimalLongitude == null) {
      //process the supplied state
      val stateTerm = StateProvinces.matchTerm(raw.location.stateProvince)
      if (!stateTerm.isEmpty) {
        processed.location.stateProvince = stateTerm.get.canonical
        processed.location.country = StateProvinceToCountry.map.getOrElse(processed.location.stateProvince, "")
      }
    }

    //Only process the raw country value if no latitude and longitude is provided
    if (processed.location.country == null && raw.location.decimalLatitude == null && raw.location.decimalLongitude == null) {
      //process the supplied state
      val countryTerm = Countries.matchTerm(raw.location.country)
      if (!countryTerm.isEmpty) {
        processed.location.country = countryTerm.get.canonical
      }
    }

    //Try the country code
    if (processed.location.country == null && raw.location.countryCode != null){
      val countryCodeTerm = Countries.matchTerm(raw.location.countryCode)
      if (!countryCodeTerm.isEmpty) {
        processed.location.country = countryCodeTerm.get.canonical
      }
    }
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
  def osGridReferenceToEastingNorthing(gridRef:String): Option[(Int, Int, Option[Int], Int, Int, Int, Int)] = {

    //deal with the 2k OS grid ref separately
    val gridRefRegex1Number = """([A-Z]{2})\s*([0-9]+)$""".r
    val gridRef2kRegex = """([A-Z]{2})\s*([0-9]+)\s*([0-9]+)\s*([A-Z]{1})""".r
    val gridRefRegex = """([A-Z]{2})\s*([0-9]+)\s*([0-9]+)$""".r
    val gridRefWithQuadRegex = """([A-Z]{2})\s*([0-9]+)\s*([0-9]+)\s*([NW|NE|SW|SE]{2})$""".r

    // validate & parse format
    val (gridletters:String, easting:String, northing:String, twoKRef:String, quadRef:String,
    coordinateUncertainty:Option[Int]) = gridRef.trim() match {
      case gridRefRegex1Number(gridletters, oneNumber) => {
        val gridDigits = oneNumber.toString
        val en = Array(gridDigits.substring(0, gridDigits.length / 2), gridDigits.substring(gridDigits.length / 2))
        val coordUncertainty = getCoordinateUncertaintyFromGridRef(gridDigits.length, 0)
        (gridletters, en(0), en(1), "", "", coordUncertainty)
      }
      case gridRefRegex(gridletters, easting, northing) => {
        (gridletters, easting, northing, "", "", getCoordinateUncertaintyFromGridRef(easting.length * 2, 0))
      }
      case gridRef2kRegex(gridletters, easting, northing, twoKRef) => {
        (gridletters, easting, northing, twoKRef, "", getCoordinateUncertaintyFromGridRef(easting.length * 2, 1))
      }
      case gridRefWithQuadRegex(gridletters, easting, northing, quadRef) => {
        (gridletters, easting, northing, "", quadRef, getCoordinateUncertaintyFromGridRef(easting.length * 2, 2))
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
    } else if (quadRef != ""){

      val cellSize = {
        if (easting.length == 1) 5000
        else if (easting.length == 2) 500
        else if (easting.length == 3) 50
        else if (easting.length == 4) 5
        else 0
      }
      if (cellSize > 0) {
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

    Some((e, n, coordinateUncertainty, e, n, e + coordinateUncertaintyOrZero, n + coordinateUncertaintyOrZero))
  }

  /**
   * Validation checks
   *
   * @param raw
   * @param processed
   * @param assertions
   */
  private def validateCoordinates(raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]): Unit = {
    if (raw.location.country == null && processed.location.country != null) {
      assertions += QualityAssertion(COUNTRY_INFERRED_FROM_COORDINATES, FAILED)
    } else {
      assertions += QualityAssertion(COUNTRY_INFERRED_FROM_COORDINATES, PASSED)
    }

    //check centre point of the state
    if (StateProvinceCentrePoints.coordinatesMatchCentre(processed.location.stateProvince, raw.location.decimalLatitude, raw.location.decimalLongitude)) {
      assertions += QualityAssertion(COORDINATES_CENTRE_OF_STATEPROVINCE, "Coordinates are centre point of " + processed.location.stateProvince)
    } else {
      assertions += QualityAssertion(COORDINATES_CENTRE_OF_STATEPROVINCE, PASSED)
    }

    //check centre point of the country
    if (CountryCentrePoints.coordinatesMatchCentre(processed.location.country, raw.location.decimalLatitude, raw.location.decimalLongitude)) {
      assertions += QualityAssertion(COORDINATES_CENTRE_OF_COUNTRY, "Coordinates are centre point of " + processed.location.country)
    } else {
      assertions += QualityAssertion(COORDINATES_CENTRE_OF_COUNTRY, PASSED)
    }
  }

  /**
   * Performs the QAs associated with elevation and depth
   */
  private def processAltitudeAndDepth(guid: String, raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]) {
    //check that the values are numeric
    processVerbatimDepth(raw, processed, assertions)
    processVerbatimElevation(raw, processed, assertions)
    processMinMaxDepth(raw, processed, assertions)
  }

  private def processMinMaxDepth(raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]): Unit = {
    //check for max and min reversals
    if (raw.location.minimumDepthInMeters != null && raw.location.maximumDepthInMeters != null) {
      try {
        val min = raw.location.minimumDepthInMeters.toFloat
        val max = raw.location.maximumDepthInMeters.toFloat
        if (min > max) {
          processed.location.minimumDepthInMeters = max.toString
          processed.location.maximumDepthInMeters = min.toString
          assertions += QualityAssertion(MIN_MAX_DEPTH_REVERSED, s"The minimum, $min and maximum, $max depths have been transposed.")
        } else {
          processed.location.minimumDepthInMeters = min.toString
          processed.location.maximumDepthInMeters = max.toString
          assertions += QualityAssertion(MIN_MAX_DEPTH_REVERSED, PASSED)
        }
      }
      catch {
        case e: Exception => logger.debug("Exception thrown processing minimumDepthInMeters:" + e.getMessage())
      }
    }

    if (raw.location.minimumElevationInMeters != null && raw.location.maximumElevationInMeters != null) {
      try {
        val min = raw.location.minimumElevationInMeters.toFloat
        val max = raw.location.maximumElevationInMeters.toFloat
        if (min > max) {
          processed.location.minimumElevationInMeters = max.toString
          processed.location.maximumElevationInMeters = min.toString
          assertions += QualityAssertion(MIN_MAX_ALTITUDE_REVERSED, s"The minimum, $min, and maximum, $max, elevations have been transposed.")
        } else {
          processed.location.minimumElevationInMeters = min.toString
          processed.location.maximumElevationInMeters = max.toString
          assertions += QualityAssertion(MIN_MAX_ALTITUDE_REVERSED, PASSED)
        }
      } catch {
        case e: Exception => logger.debug("Exception thrown processing elevation:" + e.getMessage())
      }
    }
  }

  private def processVerbatimElevation(raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]): Unit = {
    if (raw.location.verbatimElevation != null) {
      val parseElevationResult = DistanceRangeParser.parse(raw.location.verbatimElevation)
      if (parseElevationResult.isDefined) {
        val (velevation, sourceUnit) = parseElevationResult.get
        processed.location.verbatimElevation = velevation.toString
        if (velevation > 10000 || velevation < -100) {
          assertions += QualityAssertion(ALTITUDE_OUT_OF_RANGE, s"Elevation $velevation is greater than 10,000 metres or less than -100 metres.")
        } else {
          assertions += QualityAssertion(ALTITUDE_OUT_OF_RANGE, PASSED)
        }
        assertions += QualityAssertion(ALTITUDE_NON_NUMERIC, PASSED)

        if (sourceUnit == Feet) {
          assertions += QualityAssertion(ALTITUDE_IN_FEET, "The supplied altitude was in feet it has been converted to metres")
        } else {
          assertions += QualityAssertion(ALTITUDE_IN_FEET, PASSED)
        }
      } else {
        assertions += QualityAssertion(ALTITUDE_NON_NUMERIC, "Can't parse verbatimElevation " + raw.location.verbatimElevation)
      }
    }
  }

  private def processVerbatimDepth(raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]): Unit = {
    if (raw.location.verbatimDepth != null) {
      val parseDepthResult = DistanceRangeParser.parse(raw.location.verbatimDepth)
      if (parseDepthResult.isDefined) {
        val (vdepth, sourceUnit) = parseDepthResult.get
        processed.location.verbatimDepth = vdepth.toString
        if (vdepth > 10000)
          assertions += QualityAssertion(DEPTH_OUT_OF_RANGE, s"Depth $vdepth is greater than 10,000 metres")
        else
          assertions += QualityAssertion(DEPTH_OUT_OF_RANGE,  PASSED)
        assertions += QualityAssertion(DEPTH_NON_NUMERIC,  PASSED)
        //check on the units
        if (sourceUnit == Feet) {
          assertions += QualityAssertion(DEPTH_IN_FEET, "The supplied depth was in feet it has been converted to metres")
        } else {
          assertions += QualityAssertion(DEPTH_IN_FEET, PASSED)
        }
      } else {
        assertions += QualityAssertion(DEPTH_NON_NUMERIC, "Can't parse verbatimDepth " + raw.location.verbatimDepth)
      }
    }
  }

  private def setProcessedCoordinates(raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]) {

    //handle the situation where the coordinates have already been sensitised
    if (raw.location.originalDecimalLatitude != null && raw.location.originalDecimalLongitude != null) {
      processed.location.decimalLatitude = raw.location.originalDecimalLatitude
      processed.location.decimalLongitude = raw.location.originalDecimalLongitude
      processed.location.verbatimLatitude = raw.location.originalVerbatimLatitude
      processed.location.verbatimLongitude = raw.location.originalVerbatimLongitude
      //set the raw values too
      raw.location.decimalLatitude = raw.location.originalDecimalLatitude
      raw.location.decimalLongitude = raw.location.originalDecimalLongitude

    } else {
      //use raw values
      val gisPointOption = processLatLong(
        raw.location.decimalLatitude,
        raw.location.decimalLongitude,
        raw.location.geodeticDatum,
        raw.location.verbatimLatitude,
        raw.location.verbatimLongitude,
        raw.location.verbatimSRS,
        raw.location.easting,
        raw.location.northing,
        raw.location.zone,
        raw.location.gridReference,
        assertions)

      gisPointOption match {
        case Some(gisPoint) => {
          processed.location.decimalLatitude = gisPoint.latitude
          processed.location.decimalLongitude = gisPoint.longitude
          processed.location.geodeticDatum = gisPoint.datum
          processed.location.coordinateUncertaintyInMeters = gisPoint.coordinateUncertaintyInMeters
          processed.location.bbox = gisPoint.minLatitude + "," + gisPoint.minLongitude + "," + gisPoint.maxLatitude + "," + gisPoint.maxLongitude
          processed.location.northing = gisPoint.northing
          processed.location.easting = gisPoint.easting
        }
        case None => //do nothing
      }
    }
  }

  /**
   * Process the latitude, longitude converting raw coordinates to decimal latitude, longitude.
   * Handles reprojections where required.
   *
   * @param rawLatitude
   * @param rawLongitude
   * @param rawGeodeticDatum
   * @param verbatimLatitude
   * @param verbatimLongitude
   * @param verbatimSRS
   * @param easting
   * @param northing
   * @param zone
   * @param assertions
   * @return
   */
  def processLatLong(rawLatitude: String, rawLongitude: String, rawGeodeticDatum: String, verbatimLatitude: String,
                     verbatimLongitude: String, verbatimSRS: String, easting: String, northing: String, zone: String,
                     gridReference:String, assertions: ArrayBuffer[QualityAssertion]): Option[GISPoint] = {

    //check to see if we have coordinates specified
    if (rawLatitude != null && rawLongitude != null && !rawLatitude.toFloatWithOption.isEmpty && !rawLongitude.toFloatWithOption.isEmpty) {
      processDecimalCoordinates(rawLatitude, rawLongitude, rawGeodeticDatum, assertions)
      // Attempt to infer the decimal latitude and longitude from the verbatim latitude and longitude
    } else {
      //no decimal latitude/longitude was provided
      assertions += QualityAssertion(DECIMAL_COORDINATES_NOT_SUPPLIED)
      if (verbatimLatitude != null && verbatimLongitude != null) {
        var decimalVerbatimLat = verbatimLatitude.toFloatWithOption
        var decimalVerbatimLong = verbatimLongitude.toFloatWithOption

        if (decimalVerbatimLat.isEmpty || decimalVerbatimLong.isEmpty) {
          //parse the expressions into their decimal equivalents
          decimalVerbatimLat = VerbatimLatLongParser.parse(verbatimLatitude)
          decimalVerbatimLong = VerbatimLatLongParser.parse(verbatimLongitude)
        }

        if (!decimalVerbatimLat.isEmpty && !decimalVerbatimLong.isEmpty) {
          processVerbatimCoordinates(verbatimSRS, assertions, decimalVerbatimLat, decimalVerbatimLong)
        } else {
          None
        }
      } else if (easting != null && northing != null && zone != null) {
        processNorthingEastingZone(verbatimSRS, easting, northing, zone, assertions)
      } else if ( gridReference != null) {
        processGridReference(gridReference, assertions)
      } else {
        None
      }
    }
  }

  /**
   * Process supplied grid references. This currently only recognises UK OS grid references but could be
   * extended to support other systems.
   *
   * @param gridReference
   * @param assertions
   */
  def processGridReference(gridReference:String, assertions:ArrayBuffer[QualityAssertion]): Option[GISPoint] = {

    osGridReferenceToEastingNorthing(gridReference) match {
      case Some((easting, northing, coordUncertaintyOption, minE, minN, maxE, maxN)) => {

        //move coordinates to the centroid of the grid
        val reposition = if(!coordUncertaintyOption.isEmpty && coordUncertaintyOption.get > 0){
          coordUncertaintyOption.get / 2
        } else {
          0
        }

        val coords = reprojectCoordinatesToWGS84(easting + reposition, northing + reposition, Config.defaultSourceCrs, 5)

        //reproject min/max lat/lng
        val bbox = Array(
          reprojectCoordinatesToWGS84(minE, minN, Config.defaultSourceCrs, 5),
          reprojectCoordinatesToWGS84(maxE, maxN, Config.defaultSourceCrs, 5)
        )

        if(!coords.isEmpty){
          val (latitude, longitude) = coords.get
          assertions.add(QualityAssertion(DECIMAL_LAT_LONG_CALCULATED_FROM_GRID_REF))
          val uncertaintyToUse = if(!coordUncertaintyOption.isEmpty){
            coordUncertaintyOption.get.toString
          } else {
            null
          }
          Some(GISPoint(latitude,
            longitude,
            WGS84_EPSG_Code,
            uncertaintyToUse,
            easting = easting.toString,
            northing = northing.toString,
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
   * Process the raw string values supplied as decimal latitude and longitude.
   *
   * @param rawLatitude
   * @param rawLongitude
   * @param rawGeodeticDatum
   * @param assertions
   * @return
   */
  private def processDecimalCoordinates(rawLatitude: String, rawLongitude: String, rawGeodeticDatum: String,
                                        assertions: ArrayBuffer[QualityAssertion]): Option[GISPoint] = {

    //coordinates were supplied so the test passed
    assertions += QualityAssertion(DECIMAL_COORDINATES_NOT_SUPPLIED, PASSED)
    // if decimal lat/long is provided in a CRS other than WGS84, then we need to reproject

    if (rawGeodeticDatum != null) {
      //no assumptions about the datum is being made:
      assertions += QualityAssertion(GEODETIC_DATUM_ASSUMED_WGS84, PASSED)
      val sourceEpsgCode = lookupEpsgCode(rawGeodeticDatum)
      if (!sourceEpsgCode.isEmpty) {
        //datum is recognised so pass the test:
        assertions += QualityAssertion(UNRECOGNIZED_GEODETIC_DATUM, PASSED)
        if (sourceEpsgCode.get == WGS84_EPSG_Code) {
          //already in WGS84, no need to reproject
          Some(GISPoint(rawLatitude, rawLongitude, WGS84_EPSG_Code, null))
        } else {
          // Reproject decimal lat/long to WGS84
          val desiredNoDecimalPlaces = math.min(getNumberOfDecimalPlacesInDouble(rawLatitude),
            getNumberOfDecimalPlacesInDouble(rawLongitude))

          val reprojectedCoords = reprojectCoordinatesToWGS84(
            rawLatitude.toDouble,
            rawLongitude.toDouble,
            sourceEpsgCode.get,
            desiredNoDecimalPlaces
          )

          if (reprojectedCoords.isEmpty) {
            assertions += QualityAssertion(DECIMAL_LAT_LONG_CONVERSION_FAILED,
              "Transformation of decimal latitude and longitude to WGS84 failed")
            None
          } else {
            //transformation of coordinates did not fail:
            assertions += QualityAssertion(DECIMAL_LAT_LONG_CONVERSION_FAILED, PASSED)
            assertions += QualityAssertion(DECIMAL_LAT_LONG_CONVERTED,
              "Decimal latitude and longitude were converted to WGS84 (EPSG:4326)")
            val (reprojectedLatitude, reprojectedLongitude) = reprojectedCoords.get
            Some(GISPoint(reprojectedLatitude, reprojectedLongitude, WGS84_EPSG_Code, null))
          }
        }
      } else {
        assertions += QualityAssertion(UNRECOGNIZED_GEODETIC_DATUM, s"Geodetic datum $rawGeodeticDatum not recognized.")
        Some(GISPoint(rawLatitude, rawLongitude, rawGeodeticDatum, null))
      }
    } else {
      //assume coordinates already in WGS84
      assertions += QualityAssertion(GEODETIC_DATUM_ASSUMED_WGS84, "Geodetic datum assumed to be WGS84 (EPSG:4326)")
      Some(GISPoint(rawLatitude, rawLongitude, WGS84_EPSG_Code, null))
    }
  }

  /**
   * Process verbatim coordinate values.
   *
   * @param verbatimSRS
   * @param assertions
   * @param decimalVerbatimLat
   * @param decimalVerbatimLong
   * @return
   */
  private def processVerbatimCoordinates(verbatimSRS: String, assertions: ArrayBuffer[QualityAssertion],
                                 decimalVerbatimLat: Option[Float], decimalVerbatimLong: Option[Float]): Option[GISPoint] = {
    if (decimalVerbatimLat.get.toString.isLatitude && decimalVerbatimLong.get.toString.isLongitude) {

      // If a verbatim SRS is supplied, reproject coordinates to WGS 84
      if (verbatimSRS != null) {
        val sourceEpsgCode = lookupEpsgCode(verbatimSRS)
        if (!sourceEpsgCode.isEmpty) {
          //calculation from verbatim did NOT fail:
          assertions += QualityAssertion(DECIMAL_LAT_LONG_CALCULATION_FROM_VERBATIM_FAILED, PASSED)
          if (sourceEpsgCode.get == WGS84_EPSG_Code) {
            //already in WGS84, no need to reproject
            assertions += QualityAssertion(DECIMAL_LAT_LONG_CALCULATED_FROM_VERBATIM,
              "Decimal latitude and longitude were calculated using verbatimLatitude, verbatimLongitude and verbatimSRS")
            Some(GISPoint(decimalVerbatimLat.get.toString, decimalVerbatimLong.get.toString, WGS84_EPSG_Code, null))
          } else {

            val desiredNoDecimalPlaces = math.min(
              getNumberOfDecimalPlacesInDouble(decimalVerbatimLat.get.toString),
              getNumberOfDecimalPlacesInDouble(decimalVerbatimLong.get.toString)
            )

            val reprojectedCoords = reprojectCoordinatesToWGS84(decimalVerbatimLat.get, decimalVerbatimLong.get,
              sourceEpsgCode.get, desiredNoDecimalPlaces)
            if (reprojectedCoords.isEmpty) {
              assertions += QualityAssertion(DECIMAL_LAT_LONG_CALCULATION_FROM_VERBATIM_FAILED,
                "Transformation of verbatim latiude and longitude to WGS84 failed")
              None
            } else {
              //reprojection did NOT fail:
              assertions += QualityAssertion(DECIMAL_LAT_LONG_CALCULATION_FROM_VERBATIM_FAILED, PASSED)
              assertions += QualityAssertion(DECIMAL_LAT_LONG_CALCULATED_FROM_VERBATIM,
                "Decimal latitude and longitude were calculated using verbatimLatitude, verbatimLongitude and verbatimSRS")
              val (reprojectedLatitude, reprojectedLongitude) = reprojectedCoords.get
              Some(GISPoint(reprojectedLatitude, reprojectedLongitude, WGS84_EPSG_Code, null))
            }
          }
        } else {
          assertions += QualityAssertion(DECIMAL_LAT_LONG_CALCULATION_FROM_VERBATIM_FAILED, "Unrecognized verbatimSRS " + verbatimSRS)
          None
        }
        // Otherwise, assume latitude and longitude are already in WGS 84
      } else if (decimalVerbatimLat.get.toString.isLatitude && decimalVerbatimLong.get.toString.isLongitude) {
        //conversion dod NOT fail
        assertions += QualityAssertion(DECIMAL_LAT_LONG_CALCULATION_FROM_VERBATIM_FAILED, PASSED)
        assertions += QualityAssertion(DECIMAL_LAT_LONG_CALCULATED_FROM_VERBATIM,
          "Decimal latitude and longitude were calculated using verbatimLatitude, verbatimLongitude and verbatimSRS")
        Some(GISPoint(decimalVerbatimLat.get.toString, decimalVerbatimLong.get.toString, WGS84_EPSG_Code, null))
      } else {
        // Invalid latitude, longitude
        assertions += QualityAssertion(DECIMAL_LAT_LONG_CALCULATION_FROM_VERBATIM_FAILED,
          "Could not parse verbatim latitude and longitude")
        None
      }
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
  private def processNorthingEastingZone(verbatimSRS: String, easting: String, northing: String, zone: String,
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
        val reprojectedCoords = reprojectCoordinatesToWGS84(eastingAsDouble.get, northingAsDouble.get, crsEpsgCode, 5)
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
          Some(GISPoint(reprojectedLatitude, reprojectedLongitude, WGS84_EPSG_Code, null))
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

  /**
   * Re-projects coordinates into WGS 84
   *
   * @param coordinate1 first coordinate. If source value is easting/northing, then this should be the easting value.
   *                    Otherwise it should be the latitude
   * @param coordinate2 first coordinate. If source value is easting/northing, then this should be the northing value.
   *                    Otherwise it should be the longitude
   * @param sourceCrsEpsgCode epsg code for the source CRS, e.g. EPSG:4202 for AGD66
   * @param decimalPlacesToRoundTo number of decimal places to round the reprojected coordinates to
   * @return Reprojected coordinates (latitude, longitude), or None if the operation failed.
   */
  private def reprojectCoordinatesToWGS84(coordinate1: Double, coordinate2: Double, sourceCrsEpsgCode: String,
                                          decimalPlacesToRoundTo: Int): Option[(String, String)] = {
    try {
      val wgs84CRS = DefaultGeographicCRS.WGS84
      val sourceCRS = CRS.decode(sourceCrsEpsgCode)
      val transformOp = new DefaultCoordinateOperationFactory().createOperation(sourceCRS, wgs84CRS)
      val directPosition = new GeneralDirectPosition(coordinate1, coordinate2)
      val wgs84LatLong = transformOp.getMathTransform().transform(directPosition, null)

      //NOTE - returned coordinates are longitude, latitude, despite the fact that if
      //converting latitude and longitude values, they must be supplied as latitude, longitude.
      //No idea why this is the case.
      val longitude = wgs84LatLong.getOrdinate(0)
      val latitude = wgs84LatLong.getOrdinate(1)

      val roundedLongitude = Precision.round(longitude, decimalPlacesToRoundTo)
      val roundedLatitude = Precision.round(latitude, decimalPlacesToRoundTo)

      Some(roundedLatitude.toString, roundedLongitude.toString)
    } catch {
      case ex: Exception => None
    }
  }

  /**
   * Get the number of decimal places in a double value in string form
   * @param decimalAsString
   * @return
   */
   def getNumberOfDecimalPlacesInDouble(decimalAsString: String): Int = {
    val tokens = decimalAsString.split('.')
    if (tokens.length == 2) {
      tokens(1).length
    } else {
      0
    }
  }

  /**
   * Get the EPSG code associated with a coordinate reference system string e.g. "WGS84" or "AGD66".
   *
   * @param crs The coordinate reference system string.
   * @return The EPSG code associated with the CRS, or None if no matching code could be found.
   *         If the supplied string is already a valid EPSG code, it will simply be returned.
   */
  private def lookupEpsgCode(crs: String): Option[String] = {
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

  private def checkCoordinateUncertainty(raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]) {
    //validate coordinate accuracy (coordinateUncertaintyInMeters) and coordinatePrecision (precision - A. Chapman)
    var checkedPrecision =false
    if (raw.location.coordinateUncertaintyInMeters != null && raw.location.coordinateUncertaintyInMeters.length > 0) {
      //parse it into a numeric number in metres
      val parsedResult = DistanceRangeParser.parse(raw.location.coordinateUncertaintyInMeters)
      if (!parsedResult.isEmpty) {
        val (parsedValue, rawUnit) = parsedResult.get
        if(parsedValue > 0){
          //not an uncertainty mismatch
          assertions += QualityAssertion(UNCERTAINTY_RANGE_MISMATCH, PASSED)
        } else {
          val comment = "Supplied uncertainty, " + raw.location.coordinateUncertaintyInMeters + ", is not a supported format"
          assertions += QualityAssertion(UNCERTAINTY_RANGE_MISMATCH, comment)
        }
        processed.location.coordinateUncertaintyInMeters = parsedValue.toString
      } else {
        val comment = "Supplied uncertainty, " + raw.location.coordinateUncertaintyInMeters + ", is not a supported format"
        assertions += QualityAssertion(UNCERTAINTY_RANGE_MISMATCH, comment)
      }
    } else {
      //check to see if the uncertainty has incorrectly been put in the precision
      if (raw.location.coordinatePrecision != null) {
        val value = raw.location.coordinatePrecision.toFloatWithOption
        if (!value.isEmpty && value.get > 1) {
          processed.location.coordinateUncertaintyInMeters = value.get.toInt.toString
          val comment = "Supplied precision, " + raw.location.coordinatePrecision + ", is assumed to be uncertainty in metres";
          assertions += QualityAssertion(UNCERTAINTY_IN_PRECISION, comment)
          checkedPrecision = true
        }
      }
    }

    if (raw.location.coordinatePrecision == null){
      assertions += QualityAssertion(MISSING_COORDINATEPRECISION, "Missing coordinatePrecision")
    } else {
      assertions += QualityAssertion(MISSING_COORDINATEPRECISION, PASSED)
      if(!checkedPrecision){
        val value = raw.location.coordinatePrecision.toFloatWithOption
        if(value.isDefined){
          //Ensure that the precision is within the required ranges
          if (value.get > 0 && value.get <= 1){
            assertions += QualityAssertion(PRECISION_RANGE_MISMATCH, PASSED)
            //now test for coordinate precision
            val pre = if (raw.location.coordinatePrecision.contains(".")) raw.location.coordinatePrecision.split("\\.")(1).length else 0
            val lat = processed.location.decimalLatitude
            val long = processed.location.decimalLongitude
            val latp = if(lat.contains(".")) lat.split("\\.")(1).length else 0
            val lonp = if(long.contains(".")) long.split("\\.")(1).length else 0
            if(pre == latp && pre == lonp){
              // no coordinate precision mismatch exists
              assertions += QualityAssertion(COORDINATE_PRECISION_MISMATCH, PASSED)
            } else {
              assertions += QualityAssertion(COORDINATE_PRECISION_MISMATCH)
            }
          } else{
            assertions += QualityAssertion(PRECISION_RANGE_MISMATCH, "Coordinate precision is not between 0 and 1" )
          }
        } else {
           assertions += QualityAssertion(PRECISION_RANGE_MISMATCH, "Unable to parse the coordinate precision")
        }
      }
    }

    //test for coordinateUncertaintyInMeters
    if (processed.location.coordinateUncertaintyInMeters == null) {
      assertions += QualityAssertion(UNCERTAINTY_NOT_SPECIFIED, "Uncertainty was not supplied")
    } else {
      assertions += QualityAssertion(UNCERTAINTY_NOT_SPECIFIED, PASSED)
    }
  }

  /**
   * Check the habitats for the taxon profile against the biome associated with the point.
   *
   * @param raw
   * @param processed
   * @param assertions
   */
  private def checkForBiomeMismatch(raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]) {

    if (processed.location.biome == null) {
      assertions += QualityAssertion(COORDINATE_HABITAT_MISMATCH, 2)
      return
    }

    //retrieve taxon and genus profiles
    val taxonProfileWithOption = TaxonProfileDAO.getByGuid(processed.classification.taxonConceptID)
    val genusProfileWithOption = TaxonProfileDAO.getByGuid(processed.classification.genusID)
    val habitats = {
      if (!taxonProfileWithOption.isEmpty && taxonProfileWithOption.get.habitats != null && !taxonProfileWithOption.get.habitats.isEmpty) {
        taxonProfileWithOption.get.habitats
      } else if (!genusProfileWithOption.isEmpty && genusProfileWithOption.get.habitats != null && !genusProfileWithOption.get.habitats.isEmpty){
        genusProfileWithOption.get.habitats
      } else {
        Array[String]()
      }
    }

    if (!habitats.isEmpty) {
      val habitatsAsString = habitats.mkString(",")
      val habitatFromPoint = processed.location.biome
      val habitatsForSpecies = habitats
      //is "terrestrial" the same as "non-marine" ??
      val validHabitat = HabitatMap.areTermsCompatible(habitatFromPoint, habitatsForSpecies)
      if (!validHabitat.isEmpty) {
        if (!validHabitat.get) {
          logger.debug("[QualityAssertion] ******** Biomes incompatible for ROWKEY: " + raw.rowKey + ", processed:"
            + processed.location.biome + ", retrieved:" + habitatsAsString
            + ", http://maps.google.com/?ll=" + processed.location.decimalLatitude + ","
            + processed.location.decimalLongitude)
          val comment = "Recognised habitats for species: " + habitatsAsString +
            ", Value determined from coordinates: " + habitatFromPoint
          assertions += QualityAssertion(COORDINATE_HABITAT_MISMATCH, comment)
        } else {
          //habitats ARE compatible
          assertions += QualityAssertion(COORDINATE_HABITAT_MISMATCH, PASSED)
        }
      }
    } else {
      assertions += QualityAssertion(COORDINATE_HABITAT_MISMATCH, UNCHECKED)
    }
  }

  /**
   * Add the correct conservation status to the record.
   *
   * @param raw
   * @param processed
   */
  private def addConservationStatus(raw: FullRecord, processed: FullRecord) {
    //retrieve the species profile
    val taxonProfileWithOption = TaxonProfileDAO.getByGuid(processed.classification.taxonConceptID)
    if(!taxonProfileWithOption.isEmpty){
      val taxonProfile = taxonProfileWithOption.get
      //add the conservation status if necessary
      if (taxonProfile.conservation != null) {
        val country = taxonProfile.retrieveConservationStatus(processed.location.country)
        processed.occurrence.countryConservation = country.getOrElse(null)
        val state = taxonProfile.retrieveConservationStatus(processed.location.stateProvince)
        processed.occurrence.stateConservation = state.getOrElse(null)
        val global = taxonProfile.retrieveConservationStatus("Global")
        processed.occurrence.globalConservation = global.getOrElse(null)
      }
    }
  }

  /**
   * Check the supplied state value aligns with the supplied coordinates.
   *
   * @param raw
   * @param processed
   * @param assertions
   */
  private def checkForStateMismatch(raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]) {
    //check matched stateProvince
    if (processed.location.stateProvince != null && raw.location.stateProvince != null) {
      //quality systemAssertions
      val stateTerm = StateProvinces.matchTerm(raw.location.stateProvince)
      if (!stateTerm.isEmpty && !processed.location.stateProvince.equalsIgnoreCase(stateTerm.get.canonical)) {
        logger.debug("[QualityAssertion] " + raw.rowKey + ", processed:" + processed.location.stateProvince
          + ", raw:" + raw.location.stateProvince)
        //add a quality assertion
        val comment = "Supplied: " + stateTerm.get.canonical + ", calculated: " + processed.location.stateProvince
        assertions += QualityAssertion(STATE_COORDINATE_MISMATCH, comment)
      } else {
        //states are not in mismatch
        assertions += QualityAssertion(STATE_COORDINATE_MISMATCH, PASSED)
      }
    } else {
      assertions += QualityAssertion(STATE_COORDINATE_MISMATCH, UNCHECKED)
    }
  }

  /**
   * Check other geospatial details have been supplied.
   *
   * @param raw
   * @param processed
   * @param assertions
   * @return
   */
  def validateGeoreferenceValues(raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]) = {
    //check for missing geodeticDatum
    if (raw.location.geodeticDatum == null && processed.location.geodeticDatum == null)
      assertions += QualityAssertion(MISSING_GEODETICDATUM, "Missing geodeticDatum")
    else
      assertions += QualityAssertion(MISSING_GEODETICDATUM,PASSED)

    //check for missing georeferencedBy
    if (raw.location.georeferencedBy == null && processed.location.georeferencedBy == null)
      assertions += QualityAssertion(MISSING_GEOREFERNCEDBY, "Missing georeferencedBy")
    else
      assertions += QualityAssertion(MISSING_GEOREFERNCEDBY, PASSED)

    //check for missing georeferencedProtocol
    if (raw.location.georeferenceProtocol == null && processed.location.georeferenceProtocol == null)
      assertions += QualityAssertion(MISSING_GEOREFERENCEPROTOCOL, "Missing georeferenceProtocol")
    else
      assertions += QualityAssertion(MISSING_GEOREFERENCEPROTOCOL,PASSED)

    //check for missing georeferenceSources
    if (raw.location.georeferenceSources == null && processed.location.georeferenceSources == null)
      assertions += QualityAssertion(MISSING_GEOREFERENCESOURCES, "Missing georeferenceSources")
    else
      assertions += QualityAssertion(MISSING_GEOREFERENCESOURCES,PASSED)

    //check for missing georeferenceVerificationStatus
    if (raw.location.georeferenceVerificationStatus == null && processed.location.georeferenceVerificationStatus == null)
      assertions += QualityAssertion(MISSING_GEOREFERENCEVERIFICATIONSTATUS, "Missing georeferenceVerificationStatus")
    else
      assertions += QualityAssertion(MISSING_GEOREFERENCEVERIFICATIONSTATUS,PASSED)

    //check for missing georeferenceDate
    if (StringUtils.isBlank(raw.location.georeferencedDate) && !raw.miscProperties.containsKey("georeferencedDate")){
      assertions += QualityAssertion(MISSING_GEOREFERENCE_DATE)
    } else {
      assertions += QualityAssertion(MISSING_GEOREFERENCE_DATE, PASSED)
    }
  }

  /**
   * Performs a bunch of the coordinate validations
   */
  def validateCoordinatesValues(raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]) = {
    //when the locality is Australia latitude needs to be negative and longitude needs to be positive
    //TO DO fix this so that it uses the gazetteer to determine whether or not coordinates
    val latWithOption = processed.location.decimalLatitude.toFloatWithOption
    val lonWithOption = processed.location.decimalLongitude.toFloatWithOption

    if (!latWithOption.isEmpty && !lonWithOption.isEmpty) {

      val lat = latWithOption.get
      val lon = lonWithOption.get

      //Test that coordinates are in range
      if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
        //test to see if they have been inverted
        if (lon >= -90 && lon <= 90 && lat >= -180 && lat <= 180) {
          assertions += QualityAssertion(INVERTED_COORDINATES, "Assume that coordinates have been inverted. Original values: " +
            processed.location.decimalLatitude + "," + processed.location.decimalLongitude)
          val tmp = processed.location.decimalLatitude
          processed.location.decimalLatitude = processed.location.decimalLongitude
          processed.location.decimalLongitude = tmp
          //coordinates are not out of range:
          assertions += QualityAssertion(COORDINATES_OUT_OF_RANGE, PASSED)
        } else {
          assertions += QualityAssertion(COORDINATES_OUT_OF_RANGE, "Coordinates are out of range: " +
            processed.location.decimalLatitude + "," + processed.location.decimalLongitude)
          assertions += QualityAssertion(INVERTED_COORDINATES,PASSED)
        }
      } else {
        assertions ++= Array(QualityAssertion(INVERTED_COORDINATES,PASSED), QualityAssertion(COORDINATES_OUT_OF_RANGE, PASSED))
      }

      if (lat == 0.0f && lon == 0.0f) {
        assertions += QualityAssertion(ZERO_COORDINATES, "Coordinates 0,0")
        processed.location.decimalLatitude = null
        processed.location.decimalLongitude = null
      } else {
        assertions += QualityAssertion(ZERO_COORDINATES,PASSED)
      }

      if (lat == 0.0f ) {
        assertions += QualityAssertion(AssertionCodes.ZERO_LATITUDE_COORDINATES, "Latitude 0,0")
      } else{
        assertions += QualityAssertion(AssertionCodes.ZERO_LATITUDE_COORDINATES, PASSED)
      }

      if (lon == 0.0f) {
        assertions += QualityAssertion(AssertionCodes.ZERO_LONGITUDE_COORDINATES, "Longitude 0,0")
      } else{
        assertions += QualityAssertion(AssertionCodes.ZERO_LONGITUDE_COORDINATES, PASSED)
      }

      if (raw.location.country != null && raw.location.country != "") {

        val country = Countries.matchTerm(raw.location.country)

        if (!country.isEmpty) {

          assertions += QualityAssertion(UNKNOWN_COUNTRY_NAME, PASSED)

          CountryCentrePoints.matchName(country.get.canonical) match {

            case Some((latlng, bbox)) => {

              if (!bbox.containsPoint(lat, lon)) {

                var hasCoordinateMismatch = true

                if (bbox.containsPoint(lat * -1, lon)) {
                  //latitude is negated
                  assertions += QualityAssertion(NEGATED_LATITUDE,
                    "Latitude seems to be negated. Original value:" + processed.location.decimalLatitude)
                  processed.location.decimalLatitude = (lat * -1).toString
                  hasCoordinateMismatch = false
                }

                if (bbox.containsPoint(lat, lon * -1)) {
                  //point in wrong EW hemisphere - what do we do?
                  assertions += QualityAssertion(NEGATED_LONGITUDE,
                    "Longitude seems to be negated. Original value: " + processed.location.decimalLongitude)
                  processed.location.decimalLongitude = (lon * -1).toString
                  hasCoordinateMismatch = false
                }

                if(hasCoordinateMismatch){
                  assertions += QualityAssertion(COUNTRY_COORDINATE_MISMATCH)
                } else {
                  //there was no mismatch
                  assertions += QualityAssertion(COUNTRY_COORDINATE_MISMATCH, PASSED)
                }

              }
            }
            case _ => //do nothing
          }
        } else {
          assertions += QualityAssertion(UNKNOWN_COUNTRY_NAME, "Country name '" + raw.location.country + "' not recognised.")
        }
      }
    }
  }

  val crsEpsgCodesMap = {
    var valuesMap = Map[String, String]()
    for (line <- scala.io.Source.fromURL(getClass.getResource("/crsEpsgCodes.txt"), "utf-8").getLines().toList) {
      val values = line.split('=')
      valuesMap += (values(0) -> values(1))
    }
    valuesMap
  }

  val zoneEpsgCodesMap = {
    var valuesMap = Map[String, String]()
    for (line <- scala.io.Source.fromURL(getClass.getResource("/zoneEpsgCodes.txt"), "utf-8").getLines().toList) {
      val values = line.split('=')
      valuesMap += (values(0) -> values(1))
    }
    valuesMap
  }

  def getName = FullRecordMapper.geospatialQa
}


case class GISPoint(latitude:String, longitude:String, datum:String, coordinateUncertaintyInMeters:String,
                    easting:String = null, northing:String  = null, minLatitude:String = null, minLongitude:String = null, maxLatitude:String = null, maxLongitude:String = null)