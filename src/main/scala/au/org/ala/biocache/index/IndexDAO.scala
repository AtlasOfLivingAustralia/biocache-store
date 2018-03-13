package au.org.ala.biocache.index

import java.io.{File, FileWriter, OutputStream}
import java.util.Date

import au.org.ala.biocache.Config
import au.org.ala.biocache.dao.OccurrenceDAO
import au.org.ala.biocache.index.lucene.DocBuilder
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.parser.DateParser
import au.org.ala.biocache.util.Json
import au.org.ala.biocache.vocab.AssertionStatus
import com.datastax.driver.core.GettableData
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.time.{DateFormatUtils, DateUtils}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSON

/**
 * All Index implementations need to extend this trait.
 */
trait IndexDAO {

  val logger = LoggerFactory.getLogger("IndexDAO")

  //position of cassandra3 array response columns
  val columnOrder: ColumnOrder = new ColumnOrder

  def getRowKeysForQuery(query: String, limit: Int = 1000): Option[List[String]]

  def getUUIDsForQuery(query: String, limit: Int = 1000): Option[List[String]]

  def writeRowKeysToStream(query: String, outputStream: OutputStream)

  def writeUUIDsToStream(query: String, outputStream: OutputStream)

  def occurrenceDAO: OccurrenceDAO

  def getDistinctValues(query: String, field: String, max: Int): Option[List[String]]

  def pageOverFacet(proc: (String, Int) => Boolean, facetName: String, query: String, filterQueries: Array[String])

  def pageOverIndex(proc: java.util.Map[String, AnyRef] => Boolean, fieldToRetrieve: Array[String], query: String,
                    filterQueries: Array[String] = Array(), sortField: Option[String] = None,
                    sortDir: Option[String] = None, multivaluedFields: Option[Array[String]] = None)

  def pageOverIndexArray(proc: Array[AnyRef] => Boolean, fieldToRetrieve: Array[String], query: String,
                         filterQueries: Array[String] = Array(), sortField: Option[String] = None,
                         sortDir: Option[String] = None, multivaluedFields: Option[Array[String]] = None)

  def streamIndex(proc: java.util.Map[String, AnyRef] => Boolean, fieldsToRetrieve: Array[String], query: String, filterQueries: Array[String], sortFields: Array[String], multivaluedFields: Option[Array[String]] = None)

  def shouldIncludeSensitiveValue(dr: String): Boolean

  /**
   * Index a record with the supplied properties.
   */
  def indexFromMap(guid: String,
                   map: scala.collection.Map[String, String],
                   batch: Boolean = true,
                   startDate: Option[Date] = None,
                   commit: Boolean = false,
                   miscIndexProperties: Seq[String] = Array[String](),
                   userProvidedTypeMiscIndexProperties: Seq[String] = Array[String](),
                   test: Boolean = false,
                   batchID: String = "",
                   csvFileWriter: FileWriter = null,
                   csvFileWriterSensitive: FileWriter = null)

  /**
   * Truncate the current index
   */
  def emptyIndex

  def reload

  def shutdown

  def optimise : String

  def commit

  def init

  /**
   * Remove all the records with the specified value in the specified field
   */
  def removeFromIndex(field: String, values: String)

  /** Deletes all the records that satisfy the supplied query */
  def removeByQuery(query: String, commit: Boolean = true)

  /**
   * Perform
   */
  def finaliseIndex(optimise: Boolean = false, shutdown: Boolean = true)

  def getValue(field: String, map: scala.collection.Map[String, String]): String =
    if (Config.caseSensitiveCassandra) {
      map.getOrElse(field, "")
    } else {
      map.getOrElse(field.toLowerCase, "")
    }

  def getValue(field: String, map: scala.collection.Map[String, String], default: String): String =
    if (Config.caseSensitiveCassandra) {
      map.getOrElse(field, default)
    } else {
      map.getOrElse(field.toLowerCase, default)
    }

  def getParsedValue(field: String, map: scala.collection.Map[String, String]): String = getValue(field + Config.persistenceManager.fieldDelimiter + "p", map)

  def getParsedIntValue(field: String, map: scala.collection.Map[String, String]): String = {
    try {
      getValue(field + Config.persistenceManager.fieldDelimiter + "p", map).toInt.toString
    } catch {
      case e:Exception => ""
    }
  }

  def getParsedValue(field: String, map: scala.collection.Map[String, String], default: String): String = {
    val value = getValue(field + Config.persistenceManager.fieldDelimiter + "p", map)
    if (value == "") {
      default
    } else {
      value
    }
  }

  def getParsedValueIfAvailable(field: String, map: scala.collection.Map[String, String], default:String): String = {
    val value = getValue(field + Config.persistenceManager.fieldDelimiter + "p", map)
    if(value == ""){
      getValue(field, map, default)
    } else {
      value
    }
  }

  def hasParsedValue(field: String, map: scala.collection.Map[String, String]): Boolean = getValue(field + Config.persistenceManager.fieldDelimiter + "p", map) != ""

  def getValue(field: String, map: scala.collection.Map[String, String], default: String = "", checkParsed: Boolean): String = {
    val value = getValue(field, map)
    if (value == "" && checkParsed) {
      getValue(field + Config.persistenceManager.fieldDelimiter + "p", map, default)
    } else {
      value
    }
  }

  def getArrayValue(idx: Integer, array: GettableData, default: String = ""): String = {
    if (idx >= 0) {
      var value = array.getString(idx)
      if (StringUtils.isEmpty(value)) {
        value = default
      }
      value
    } else {
      default
    }
  }

  /**
   * Returns a lat,long string expression formatted to the supplied Double format
   */
  def getLatLongString(lat: Double, lon: Double, format: String): String = {
    if (!lat.isNaN && !lon.isNaN) {
      val df = new java.text.DecimalFormat(format)
      //By some "strange" decision the default rounding model is HALF_EVEN
      df.setRoundingMode(java.math.RoundingMode.HALF_UP)
      df.format(lat) + "," + df.format(lon)
    } else {
      ""
    }
  }

  /**
   * Returns a lat,long string expression formatted to the supplied Double format and step size.
   *
   * e.g. 0.02 step size
   *
   */
  def getLatLongStringStep(lat: Double, lon: Double, format: String, step: Double): String = {
    if (!lat.isNaN && !lon.isNaN) {
      val df = new java.text.DecimalFormat(format)
      //By some "strange" decision the default rounding model is HALF_EVEN
      df.setRoundingMode(java.math.RoundingMode.HALF_UP)
      df.format(Math.round(lat / step) * step) + "," + df.format(Math.round(lon / step) * step)
    } else {
      ""
    }
  }

  /**
    * header attributes used by index-local-node-v2
    *
    * Cassandra column name -> (solr field name, (0=date, 4=multivalue, -1=default), (0=both, 2=raw, 3=parsed))
    *
    * TODO: 1. Convert non-DWC SOLR field names to DWC
    * TODO: 2. Simplify to CassandraColumnName -> SolrFieldName. Complexity is required to reflect backward compatibility.
    * TODO: 3. Remove all DWC fields. These should be indexed by default.
    */
  lazy val headerAttributes = List(("dateIdentified", "identified_date", 0, 3),
    ("firstLoaded", "first_loaded_date", 0, 2),
    (FullRecordMapper.alaModifiedColumn, "last_load_date", 0, 2),
    (FullRecordMapper.alaModifiedColumn, "last_processed_date", 0, 3),
    (FullRecordMapper.lastUserAssertionDateColumn, "last_assertion_date", 0, 2),
    ("eventDate", "occurrence_date", 0, 3),
    ("eventDateEnd", "occurrence_date_end_dt", 0, 3),
    ("loanDate", "loan_date", 0, 2),
    ("loanReturnDate", "loan_return_date", 0, 2),
    ("modified", "modified_date", 0, 3),

    ("dataHubUid", "data_hub_uid", 4, 0),
    ("speciesGroups", "species_group", 4, 3),
    ("interactions", "interaction", 4, 3),
    ("taxonomicIssue", "taxonomic_issue", 4, 3),
    ("speciesHabitats", "species_habitats", 4, 3),
    ("duplicationType", "duplicate_type", 4, 3),
    ("establishmentMeans", "establishment_means", 4, 3),

    ("year", "occurrence_year", 0, 3),

    ("dataProviderName", "data_provider", -1, 0),
    ("dataProviderUid", "data_provider_uid", -1, 0),
    ("dataResourceName", "data_resource", -1, 0),
    ("dataResourceUid", "data_resource_uid", -1, 0),

    ("catalogNumber", "catalogue_number", -1, 2),
    ("collectionCode", "collection_code", -1, 2),
    ("countryCode", "country_code", -1, 2),
    ("datasetName", "dataset_name", -1, 2),
    ("datePrecision", "date_precision", -1, 2),
    ("decimalLatitude", "raw_latitude", -1, 2),
    ("decimalLongitude", "raw_longitude", -1, 2),
    ("duplicates", "duplicate_inst", -1, 2),
    ("eventID", "event_id", -1, 2),
    (FullRecordMapper.taxonomicDecisionColumn, "taxonomic_kosher", -1, 2),
    ("geodeticDatum", "raw_datum", -1, 2),
    ("georeferenceVerificationStatus", "raw_geo_validation_status", -1, 2),
    ("identificationQualifier", "raw_identification_qualifier", -1, 2),
    ("identifiedBy", "identified_by", -1, 2),
    ("individualCount", "individual_count", -1, 2),
    ("institutionCode", "institution_code", -1, 2),
    ("loanDestination", "loan_destination", -1, 2),
    ("loanForBotanist", "loan_botanist", -1, 2),
    ("loanIdentifier", "loan_identifier", -1, 2),
    ("loanSequenceNumber", "loan_number", -1, 2),
    ("locality", "raw_locality", -1, 2),
    ("locationID", "location_id", -1, 2),
    ("locationRemarks", "location_remarks", -1, 2),
    ("occurrenceDetails", "occurrence_details", -1, 2),
    ("occurrenceID", "occurrence_id", -1, 2),
    ("occurrenceRemarks", "occurrence_remarks", -1, 2),
    ("occurrenceStatus", "raw_occurrence_status", -1, 2),
    ("originalNameUsage", "original_name_usage", -1, 2),
    ("phenology", "life_stage", -1, 2),
    ("photographer", "photographer", -1, 2),
    ("recordedBy", "collector", -1, 2),
    ("recordNumber", "record_number", -1, 2),
    ("reproductiveCondition", "reproductive_condition", -1, 2),
    ("rights", "rights", -1, 2),
    ("rowkey", "row_key", -1, 2),
    ("sex", "raw_sex", -1, 2),
    ("taxonConceptID", "taxon_concept_lsid", -1, 2),
    ("typeStatus", "raw_type_status", -1, 2),
    ("userId", "user_id", -1, 2),
    ("userId", "alau_user_id", -1, 2),
    ("uuid", "id", -1, 2),
    ("vernacularName", "raw_common_name", -1, 2),
    ("distanceOutsideExpertRange", "distance_outside_expert_range", -1, 3),
    ("associatedOccurrences", "duplicate_record", -1, 3),
    ("basisOfRecord", "basis_of_record", -1, 3),
    ("classs", "class", -1, 3),
    ("collectionName", "collection_name", -1, 3),
    ("collectionUid", "collection_uid", -1, 3),
    ("coordinateUncertaintyInMeters", "coordinate_uncertainty", -1, 3),
    ("country", "country", -1, 3),
    ("dataHub", "data_hub", -1, 3),
    ("decimalLatitude", "latitude", -1, 3),
    ("decimalLongitude", "longitude", -1, 3),
    ("duplicationStatus", "duplicate_status", -1, 3),
    ("family", "family", -1, 3),
    ("georeferenceVerificationStatus", "georeference_verification_status", -1, 3),
    ("genus", "genus", -1, 3),
    ("genusID", "genus_guid", -1, 3),
    ("identificationQualifier", "identification_qualifier", -1, 3),
    ("identificationVerificationStatus", "identification_verification_status", -1, 3),
    ("institutionName", "institution_name", -1, 3),
    ("institutionUid", "institution_uid", -1, 3),
    ("kingdom", "kingdom", -1, 3),
    ("left", "lft", -1, 3),
    ("lga", "places", -1, 3),
    ("license", "license", -1, 3),
    ("maximumDepthInMeters", "max_depth_d", -1, 3),
    ("maximumElevationInMeters", "max_elevation_d", -1, 3),
    ("minimumDepthInMeters", "min_depth_d", -1, 3),
    ("minimumElevationInMeters", "min_elevation_d", -1, 3),
    ("month", "month", -1, 3),
    ("nameMatchMetric", "name_match_metric", -1, 3),
    ("nameParseType", "name_parse_type", -1, 3),
    ("occurrenceStatus", "occurrence_status", -1, 3),
    ("order", "order", -1, 3),
    ("phylum", "phylum", -1, 3),
    ("provenance", "provenance", -1, 3),
    ("recordedBy", "collectors", -1, 3),
    ("right", "rgt", -1, 3),
    ("scientificName", "taxon_name", -1, 3),
    ("species", "species", -1, 3),
    ("speciesID", "species_guid", -1, 3),
    ("stateProvince", "state", -1, 3),
    ("taxonRank", "rank", -1, 3),
    ("taxonRankID", "rank_id", -1, 3),
    ("typeStatus", "type_status", -1, 3),
    ("verbatimDepth", "depth", -1, 3),
    ("verbatimElevation", "elevation", -1, 3),
    ("vernacularName", "common_name", -1, 3),
    ("year", "year", -1, 3))

  /**
    * headerAttributesFix are the unprocessed fields excluded as a result of the backwards compatible headerAttributes.
    *
    * These fields are not indexed by index-local-node-v2 for sensitive records.
    */
  lazy val headerAttributesFix = List(
    ("verbatimElevation", "raw_min_elevation", -1, 2), // NEW
    ("verbatimDepth", "raw_min_depth_d", -1, 2), // NEW
    ("taxonRank", "raw_rank", -1, 2), // NEW
    ("stateProvince", "raw_state", -1, 2), // NEW
    ("scientificName", "raw_taxon_name", -1, 2), // NEW
    ("phylum", "raw_phylum", -1, 2), // NEW
    ("order", "raw_order", -1, 2), // NEW
    ("month", "raw_month", -1, 2), // NEW
    ("minimumElevationInMeters", "raw_min_elevation", -1, 2), // NEW
    ("minimumDepthInMeters", "raw_min_depth", -1, 2), // NEW
    ("maximumElevationInMeters", "raw_max_elevation", -1, 2), // NEW
    ("maximumDepthInMeters", "raw_max_depth", -1, 2), // NEW
    ("license", "raw_license", -1, 2), // NEW
    ("kingdom", "raw_kingdom", -1, 2), // NEW
    ("genus", "raw_genus", -1, 2), // NEW
    ("family", "raw_family", -1, 2), // NEW
    ("country", "raw_country", -1, 2), // NEW
    ("coordinateUncertaintyInMeters", "raw_coordinate_uncertainty", -1, 2), // NEW
    ("classs", "raw_class", -1, 2), // NEW
    ("basisOfRecord", "raw_basis_of_record", -1, 2), // NEW
    ("associatedOccurrences", "raw_duplicate_record", -1, 2), // NEW
    ("year", "raw_occurrence_year", 0, 2), // NEW
    ("establishmentMeans", "raw_establishment_means", 4, 2), // NEW
    ("dateIdentified", "raw_identified_date", 0, 2),  // NEW
    ("eventDate", "raw_occurrence_date", 0, 2),  // NEW
    ("eventDateEnd", "raw_occurrence_date_end_dt", 0, 2),  // NEW
    ("modified", "raw_modified_date", 0, 2)) // NEW

  /**
   * The header values for the CSV file.
   */
  lazy val header = List("id", "occurrence_id", "data_hub_uid", "data_hub", "data_provider_uid", "data_provider", "data_resource_uid",
    "data_resource", "institution_uid", "institution_code", "institution_name",
    "collection_uid", "collection_code", "collection_name", "catalogue_number",
    "taxon_concept_lsid", "occurrence_date", "occurrence_date_end_dt", "occurrence_year", "occurrence_decade_i", "taxon_name", "common_name", "names_and_lsid", "common_name_and_lsid",
    "rank", "rank_id", "raw_taxon_name", "raw_common_name", "multimedia", "image_url", "all_image_url",
    "species_group", "country_code", "country", "lft", "rgt", "kingdom", "phylum", "class", "order",
    "family", "genus", "genus_guid", "species", "species_guid", "state", "places", "latitude", "longitude",
    "lat_long", "point-1", "point-0.1", "point-0.01", "point-0.02", "point-0.001", "point-0.0001",
    "year", "month", "basis_of_record", "raw_basis_of_record", "type_status",
    "raw_type_status", "taxonomic_kosher", "geospatial_kosher", "location_remarks",
    "occurrence_remarks", "user_assertions", "collector", "state_conservation", "raw_state_conservation", "country_conservation", "raw_country_conservation",
    "sensitive", "coordinate_uncertainty", "user_id", "alau_user_id", "provenance", "subspecies_guid", "subspecies_name", "interaction", "last_assertion_date",
    "last_load_date", "last_processed_date", "modified_date", "establishment_means", "loan_number", "loan_identifier", "loan_destination",
    "loan_botanist", "loan_date", "loan_return_date", "original_name_usage", "duplicate_inst", "record_number", "first_loaded_date", "name_match_metric",
    "life_stage", "outlier_layer", "outlier_layer_count", "taxonomic_issue", "raw_identification_qualifier", "identification_qualifier", "species_habitats",
    "identified_by", "identified_date", "sensitive_longitude", "sensitive_latitude", "pest_flag", "collectors", "duplicate_status", "duplicate_record",
    "duplicate_type", "sensitive_coordinate_uncertainty", "distance_outside_expert_range", "elevation_d", "min_elevation_d", "max_elevation_d",
    "depth_d", "min_depth_d", "max_depth_d", "name_parse_type", "occurrence_status", "occurrence_details", "photographer", "rights",
    "raw_geo_validation_status", "raw_occurrence_status", "raw_locality", "raw_latitude", "raw_longitude", "raw_datum", "raw_sex",
    "sensitive_locality", "event_id", "location_id", "dataset_name", "reproductive_condition", "license", "individual_count", "date_precision",
    "identification_verification_status", "georeference_verification_status"

  ) ::: Config.additionalFieldsToIndex

  /**
   * sensitive csv header columns
   */
  val sensitiveHeader = List("sensitive_longitude", "sensitive_latitude", "sensitive_coordinate_uncertainty", "sensitive_locality")

  /**
   * Constructs a scientific name.
   *
   * TODO Factor this out of indexing logic, and have a separate field in cassandra that stores this.
   * TODO Construction of this field can then happen as part of the processing.
   */
  def getRawScientificName(map: scala.collection.Map[String, String]) : String = {
    val scientificName: String = {

      val sciName = getValue("scientificName", map, "")
      val genus = getValue("genus", map, "")
      val family = getValue("family", map, "")

      if (sciName != "") {
        sciName
      } else if (genus != "") {
        var tmp = getValue("genus", map, "")
        val specificEpithet = getValue("specificEpithet", map, "")
        val species = getValue("species", map, "")
        val infraspecificEpithet = getValue("infraspecificEpithet", map, "")
        val subspecies = getValue("subspecies", map, "")

        if (specificEpithet != "" || species != "") {
          if (specificEpithet != "")
            tmp = tmp + " " + specificEpithet
          else
            tmp = tmp + " " + species

        }

        if (infraspecificEpithet != "" || subspecies != "") {
          if (infraspecificEpithet != "")
            tmp = tmp + " " + infraspecificEpithet
          else
            tmp = tmp + " " + subspecies
        }

        tmp
      } else {
        family
      }
    }
    scientificName
  }

  /**
   * Generates an string array version of the occurrence model.
   *
   * Access to the values are taken directly from the Map with no reflection. This
   * should result in a quicker load time.
   */
  def getOccIndexModel(guid: String, map: scala.collection.Map[String, String]) : List[String] = {

    try {
      //get the lat lon values so that we can determine all the point values
      val deleted = getValue(FullRecordMapper.deletedColumn, map, "false")
      //only add it to the index is it is not deleted & not a blank record
      if (!deleted.equals("true") && map.size > 1) {
        var slat = getParsedValue("decimalLatitude", map)
        var slon = getParsedValue("decimalLongitude", map)
        var latlon = ""
        val sciName = getParsedValue("scientificName", map)
        val taxonConceptId = getParsedValue("taxonConceptID", map)
        val vernacularName = getParsedValue("vernacularName", map).trim
        val kingdom = getParsedValue("kingdom", map)
        val family = getParsedValue("family", map)
        val images = {
          val simages = getValue("images", map)
          if (!simages.isEmpty)
            Json.toStringArray(simages)
          else
            Array[String]()
        }
        //determine the type of multimedia that is available.
        val multimedia: Array[String] = {
          val i = map.getOrElse("images", "[]")
          val s = map.getOrElse("sounds", "[]")
          val v = map.getOrElse("videos", "[]")
          val ab = new ArrayBuffer[String]
          if (i.length() > 3) ab += "Image"
          if (s.length() > 3) ab += "Sound"
          if (v.length() > 3) ab += "Video"
          if (!ab.isEmpty) {
            ab.toArray
          } else {
            Array("None")
          }
        }
        val speciesGroup = {
          val sspeciesGroup = getParsedValue("speciesGroups", map)
          if (sspeciesGroup.length > 0)
            Json.toStringArray(sspeciesGroup)
          else
            Array[String]()
        }
        val interactions = {
          if (hasParsedValue("interactions", map))
            Json.toStringArray(getParsedValue("interactions", map))
          else
            Array[String]()
        }
        val dataHubUids = {
          val sdatahubs = getValue("dataHubUid", map, checkParsed = true)
          if (sdatahubs.length > 0)
            Json.toStringArray(sdatahubs)
          else
            Array[String]()
        }
        val habitats = Json.toStringArray(getParsedValue("speciesHabitats", map, "[]"))

        var eventDate = getParsedValue("eventDate", map)
        var eventDateEnd = getParsedValue("eventDateEnd", map)
        var occurrenceYear = getParsedIntValue("year", map)
        var occurrenceDecade = ""
        if (occurrenceYear.length == 4) {
          occurrenceYear += "-01-01T00:00:00Z"
          occurrenceDecade = occurrenceYear.substring(0, 3) + "0"
        } else
          occurrenceYear = ""
        //only want to include eventDates that are in the correct format
        try {
          DateUtils.parseDate(eventDate, Array("yyyy-MM-dd"))
        } catch {
          case e: Exception => eventDate = ""
        }
        //only want to include eventDateEnds that are in the correct format
        try {
          DateUtils.parseDate(eventDateEnd, Array("yyyy-MM-dd"))
        } catch {
          case e: Exception => eventDateEnd = ""
        }
        var lat = java.lang.Double.NaN
        var lon = java.lang.Double.NaN

        if (slat != "" && slon != "") {
          try {
            lat = java.lang.Double.parseDouble(slat)
            lon = java.lang.Double.parseDouble(slon)
            val test = -90D
            val test2 = -180D
            //ensure that the lat longs are in the required range before
            if (lat <= 90 && lat >= test && lon <= 180 && lon >= test2) {
              latlon = slat + "," + slon
            }
          } catch {
            //If the latitude or longitude can't be parsed into a double we don't want to index the values
            case e: Exception => slat = ""; slon = ""
          }
        }
        //get sensitive values map
        val sensitiveMap = {
          if (shouldIncludeSensitiveValue(getValue("dataResourceUid", map)) && map.contains("originalSensitiveValues")) {
            try {
              val osv = getValue("originalSensitiveValues", map, "{}")
              val parsed = JSON.parseFull(osv)
              parsed.get.asInstanceOf[Map[String, String]]
            } catch {
              case _: Exception => Map[String, String]()
            }
          } else {
            Map[String, String]()
          }
        }
        val sconservation = getParsedValue("stateConservation", map)
        var stateCons = if (sconservation != "") sconservation.split(",")(0) else ""
        val rawStateCons = if (sconservation != "") {
          val sconversations = sconservation.split(",")
          if (sconversations.length > 1)
            sconversations(1)
          else
            ""
        } else ""

        if (stateCons == "null") stateCons = rawStateCons

        val cconservation = getParsedValue("countryConservation", map)
        var countryCons = if (cconservation != "") cconservation.split(",")(0) else ""
        val rawCountryCons = if (cconservation != "") {
          val cconservations = cconservation.split(",")
          if (cconservations.length > 1)
            cconservations(1)
          else
            ""
        } else ""

        if (countryCons == "null") countryCons = rawCountryCons

        val sensitive: String = {
          val dataGen = getParsedValue("dataGeneralizations", map)
          if (dataGen.contains("already generalised"))
            "alreadyGeneralised"
          else if (dataGen != "")
            "generalised"
          else
            ""
        }

        val outlierForLayers: Array[String] = {
          val outlierForLayerStr = getParsedValue("outlierForLayers", map)
          if (outlierForLayerStr != "") {
            try {
              Json.toStringArray(outlierForLayerStr)
            } catch {
              case e: Exception => logger.warn(e.getMessage + " : " + guid); Array[String]()
            }
          }
          else Array()
        }

        val dupTypes: Array[String] = {
          val s = getParsedValue("duplicationType", map, "[]")
          try {
            Json.toStringArray(s)
          } catch {
            case e: Exception => logger.warn(e.getMessage + " : " + guid); Array[String]()
          }
        }

        //Only set the geospatially kosher field if there are coordinates supplied
        val geoKosher = if (slat == "" && slon == "") "" else getValue(FullRecordMapper.geospatialDecisionColumn, map, "")

        //val hasUserAss = map.getOrElse(FullRecordMapper.userQualityAssertionColumn, "")
        val userAssertionStatus: Int = getValue(FullRecordMapper.userAssertionStatusColumn, map, AssertionStatus.QA_NONE.toString).toInt
        val hasUserAss: String = userAssertionStatus match {
          case AssertionStatus.QA_NONE => ""
          case _ => userAssertionStatus.toString
        }

        val (subspeciesGuid, subspeciesName): (String, String) = {
          if (hasParsedValue("taxonRankID", map)) {
            try {
              if (java.lang.Integer.parseInt(getParsedValue("taxonRankID", map, "")) > 7000)
                (taxonConceptId, sciName)
              else
                ("", "")
            } catch {
              case _: Exception => ("", "")
            }
          } else {
            ("", "")
          }
        }

        val lastLoaded = DateParser.parseStringToDate(getValue(FullRecordMapper.alaModifiedColumn, map))

        val lastProcessed = DateParser.parseStringToDate(getParsedValue(FullRecordMapper.alaModifiedColumn, map))

        val lastUserAssertion = DateParser.parseStringToDate(getValue(FullRecordMapper.lastUserAssertionDateColumn, map, ""))

        val firstLoadDate = DateParser.parseStringToDate(getValue("firstLoaded", map))

        val loanDate = DateParser.parseStringToDate(getValue("loanDate", map, ""))

        val loanReturnDate = DateParser.parseStringToDate(getValue("loanReturnDate", map, ""))

        val dateIdentified = DateParser.parseStringToDate(getParsedValue("dateIdentified", map))

        val modifiedDate = DateParser.parseStringToDate(getParsedValue("modified", map))

        var taxonIssue = getParsedValue("taxonomicIssue", map, "[]")
        if (!taxonIssue.startsWith("[")) {
          logger.warn("WARNING " + map.getOrElse("rowkey", "") + " does not have an updated taxonIssue: " + guid)
          taxonIssue = "[]"
        }
        val taxonIssueArray = Json.toStringArray(taxonIssue)
        val infoWith = getParsedValue("informationWithheld", map)
        val pest_tmp = if (infoWith.contains("\t")) infoWith.substring(0, infoWith.indexOf("\t")) else "" //startsWith("PEST")) "PEST" else ""

        var distanceOutsideExpertRange = getParsedValue("distanceOutsideExpertRange", map)
        //only want valid numbers
        try {
          distanceOutsideExpertRange.toDouble
        } catch {
          case e: Exception => distanceOutsideExpertRange = ""
        }

        //the returned list needs to match up with the CSV header
        return List[String](
          getValue("rowkey", map),
          getValue("occurrenceID", map),
          dataHubUids.mkString("|"),
          getParsedValue("dataHub", map),
          getValue("dataProviderUid", map, checkParsed = true),
          getValue("dataProviderName", map, checkParsed = true),
          getValue("dataResourceUid", map, checkParsed = true),
          getValue("dataResourceName", map, checkParsed = true),
          getParsedValue("institutionUid", map),
          getValue("institutionCode", map),
          getParsedValue("institutionName", map),
          getParsedValue("collectionUid", map),
          getValue("collectionCode", map),
          getParsedValue("collectionName", map),
          getValue("catalogNumber", map),
          taxonConceptId,
          if (eventDate != "") eventDate + "T00:00:00Z" else "",
          if (eventDateEnd != "") eventDateEnd + "T00:00:00Z" else "",
          occurrenceYear,
          occurrenceDecade,
          sciName,
          vernacularName,
          sciName + "|" + taxonConceptId + "|" + vernacularName + "|" + kingdom + "|" + family,
          vernacularName + "|" + sciName + "|" + taxonConceptId + "|" + vernacularName + "|" + kingdom + "|" + family,
          getParsedValue("taxonRank", map),
          getParsedValue("taxonRankID", map),
          getRawScientificName(map),
          getValue("vernacularName", map),
          //if (!images.isEmpty && images(0) != "") "Multimedia" else "None",
          multimedia.mkString("|"),
          if (!images.isEmpty) images(0) else "",
          images.mkString("|"),
          speciesGroup.mkString("|"),
          getValue("countryCode", map),
          getParsedValue("country", map),
          getParsedValue("left", map),
          getParsedValue("right", map),
          kingdom,
          getParsedValue("phylum", map),
          getParsedValue("classs", map),
          getParsedValue("order", map),
          family,
          getParsedValue("genus", map),
          getParsedValue("genusID", map),
          getParsedValue("species", map),
          getParsedValue("speciesID", map),
          getParsedValue("stateProvince", map),
          getParsedValue("lga", map),
          slat,
          slon,
          latlon,
          getLatLongString(lat, lon, "#"),
          getLatLongString(lat, lon, "#.#"),
          getLatLongString(lat, lon, "#.##"),
          getLatLongStringStep(lat, lon, "#.##", 0.02),
          getLatLongString(lat, lon, "#.###"),
          getLatLongString(lat, lon, "#.####"),
          getParsedValue("year", map),
          getParsedValue("month", map),
          getParsedValue("basisOfRecord", map),
          getValue("basisOfRecord", map),
          getParsedValue("typeStatus", map),
          getValue("typeStatus", map),
          getValue(FullRecordMapper.taxonomicDecisionColumn, map),
          geoKosher,
          //NC 2013-05-23: Assertions are now values, failed, passed and untested these will be handled separately
          //getAssertions(map).mkString("|"),
          getValue("locationRemarks", map),
          getValue("occurrenceRemarks", map),
          hasUserAss,
          //  userAssertionStatus,
          getValue("recordedBy", map),
          stateCons, //stat
          rawStateCons,
          countryCons,
          rawCountryCons,
          sensitive,
          getParsedIntValue("coordinateUncertaintyInMeters", map),
          getValue("userId", map, ""),
          getValue("userId", map, ""),
          getParsedValue("provenance", map),
          subspeciesGuid,
          subspeciesName,
          interactions.mkString("|"),
          if (lastUserAssertion.isEmpty) "" else DateFormatUtils.format(lastUserAssertion.get, "yyyy-MM-dd'T'HH:mm:ss'Z'"),
          if (lastLoaded.isEmpty) "2010-11-1T00:00:00Z" else DateFormatUtils.format(lastLoaded.get, "yyyy-MM-dd'T'HH:mm:ss'Z'"),
          if (lastProcessed.isEmpty) "" else DateFormatUtils.format(lastProcessed.get, "yyyy-MM-dd'T'HH:mm:ss'Z'"),
          if (modifiedDate.isEmpty) "" else DateFormatUtils.format(modifiedDate.get, "yyy-MM-dd'T'HH:mm:ss'Z'"),
          getParsedValue("establishmentMeans", map).replaceAll("; ", "|"),
          getValue("loanSequenceNumber", map, ""),
          getValue("loanIdentifier", map, ""),
          getValue("loanDestination", map, ""),
          getValue("loanForBotanist", map, ""),
          if (loanDate.isEmpty) "" else DateFormatUtils.format(loanDate.get, "yyyy-MM-dd'T'HH:mm:ss'Z'"),
          if (loanReturnDate.isEmpty) "" else DateFormatUtils.format(loanReturnDate.get, "yyyy-MM-dd'T'HH:mm:ss'Z'"),
          getValue("originalNameUsage", map, getValue("typifiedName", map, "")),
          getValue("duplicates", map, ""), //.replaceAll(",","|"),
          getValue("recordNumber", map, ""),
          if (firstLoadDate.isEmpty) "" else DateFormatUtils.format(firstLoadDate.get, "yyyy-MM-dd'T'HH:mm:ss'Z'"),
          getParsedValue("nameMatchMetric", map),
          getValue("phenology", map, ""),
          outlierForLayers.mkString("|"),
          outlierForLayers.length.toString,
          taxonIssueArray.mkString("|"),
          getValue("identificationQualifier", map),
          getParsedValue("identificationQualifier", map),
          habitats.mkString("|"),
          getValue("identifiedBy", map),
          if (dateIdentified.isEmpty) "" else DateFormatUtils.format(dateIdentified.get, "yyyy-MM-dd'T'HH:mm:ss'Z'"),
          sensitiveMap.getOrElse("decimalLongitude", ""),
          sensitiveMap.getOrElse("decimalLatitude", ""),
          pest_tmp,
          getParsedValue("recordedBy", map),
          getParsedValue("duplicationStatus", map),
          getParsedValue("associatedOccurrences", map),
          dupTypes.mkString("|"),
          getParsedValue("coordinateUncertaintyInMeters", sensitiveMap),
          distanceOutsideExpertRange,
          getParsedValue("verbatimElevation", map),
          getParsedValue("minimumElevationInMeters", map),
          getParsedValue("maximumElevationInMeters", map),
          getParsedValue("verbatimDepth", map),
          getParsedValue("minimumDepthInMeters", map),
          getParsedValue("maximumDepthInMeters", map),
          getParsedValue("nameParseType", map),
          getParsedValue("occurrenceStatus", map),
          getValue("occurrenceDetails", map),
          getValue("photographer", map),
          getValue("rights", map),
          getValue("georeferenceVerificationStatus", map),
          getValue("occurrenceStatus", map),
          getValue("locality", map),
          getValue("decimalLatitude", map),
          getValue("decimalLongitude", map),
          getValue("geodeticDatum", map),
          getValue("sex", map),
          getValue("locality", sensitiveMap),
          getValue("eventID", map),
          getValue("locationID", map),
          getValue("datasetName", map),
          getValue("reproductiveCondition", map),
          getParsedValue("license", map),
          getValue("individualCount", map),
          getParsedValueIfAvailable("datePrecision", map, ""),
          getParsedValue("identificationVerificationStatus", map),
          getParsedValue("georeferenceVerificationStatus", map)
        ) ::: Config.additionalFieldsToIndex.map(field => getValue(field, map, ""))
      } else {
        return List()
      }
    } catch {
      case e: Exception => logger.error(e.getMessage, e); throw e
    }
  }

  def getCsvWriter(sensitive: Boolean = false) = {
    var fw: FileWriter = null
    if (!sensitive && Config.exportIndexAsCsvPath != null && Config.exportIndexAsCsvPath.length > 0) {
      fw = new FileWriter(File.createTempFile("index.", "." + System.currentTimeMillis() + ".csv", new File(Config.exportIndexAsCsvPath)))
    }
    if (sensitive && Config.exportIndexAsCsvPathSensitive != null && Config.exportIndexAsCsvPathSensitive.length > 0) {
      fw = new FileWriter(File.createTempFile("index.sensitive.", "." + System.currentTimeMillis() + ".csv", new File(Config.exportIndexAsCsvPathSensitive)))
    }
    fw
  }

  def writeOccIndexModelToDoc(doc: DocBuilder, guid: String, map: scala.collection.Map[String, String]) = {
    try {
      //get the lat lon values so that we can determine all the point values
      val deleted = getValue(FullRecordMapper.deletedColumn, map)
      //only add it to the index is it is not deleted & not a blank record
      if (!deleted.equals("true") && map.size > 1) {

        var slat = getParsedValue("decimalLatitude", map)
        var slon = getParsedValue("decimalLongitude", map)
        var latlon = ""
        val sciName = getParsedValue("scientificName", map)
        val taxonConceptId = getParsedValue("taxonConceptID", map)
        val vernacularName = getParsedValue("vernacularName", map).trim
        val kingdom = getParsedValue("kingdom", map)
        val family = getParsedValue("family", map)

        val simages = getValue("images", map)
        if (StringUtils.isNotEmpty(simages)) {
          jsonArrayLoop(simages, (item, idx) => {
            if (idx == 0) {
              doc.getDoc.get("image_url")
              addField(doc, "all_image_url", item)
            }
            addField(doc, "all_image_url", item)
          })
        }

        //determine the type of multimedia that is available.
        val im = getValue("images", map)
        val s = getValue("sounds", map)
        val v = getValue("videos", map)
        if (im.length() > 0) addField(doc, "multimedia", "Image")
        if (s.length() > 0) addField(doc, "multimedia", "Sound")
        if (v.length() > 0) addField(doc, "multimedia", "Video")

        val sinteractions = getParsedValue("interactions", map)
        if (StringUtils.isNotEmpty(sinteractions))
          jsonArrayLoop(sinteractions, (item, idx) => {
            addField(doc, "interaction", item)
          })

        val sdatahubs = getValue("dataHubUid", map, checkParsed = true)
        if (StringUtils.isNotEmpty(sdatahubs))
          jsonArrayLoop(sdatahubs, (item, idx) => {
            addField(doc, "data_hub_uid", item)
          })

        val habitats = getParsedValue("speciesHabitats", map)
        if (StringUtils.isNotEmpty(sdatahubs))
          jsonArrayLoop(habitats, (item, idx) => {
            addField(doc, "species_habitats", item)
          })

        var eventDate = getParsedValue("eventDate", map)
        var eventDateEnd = getParsedValue("eventDateEnd", map)
        var occurrenceYear = getParsedValue("year", map)
        var occurrenceDecade = ""
        if (occurrenceYear.length == 4) {
          occurrenceYear += "-01-01T00:00:00Z"
          occurrenceDecade = occurrenceYear.substring(0, 3) + "0"
        } else
          occurrenceYear = ""
        //only want to include eventDates that are in the correct format
        try {
          DateUtils.parseDate(eventDate, Array("yyyy-MM-dd"))
        } catch {
          case e: Exception => eventDate = ""
        }
        //only want to include eventDateEnds that are in the correct format
        try {
          DateUtils.parseDate(eventDateEnd, Array("yyyy-MM-dd"))
        } catch {
          case e: Exception => eventDateEnd = ""
        }
        var lat = java.lang.Double.NaN
        var lon = java.lang.Double.NaN

        if (slat != "" && slon != "") {
          try {
            lat = java.lang.Double.parseDouble(slat)
            lon = java.lang.Double.parseDouble(slon)
            val test = -90D
            val test2 = -180D
            //ensure that the lat longs are in the required range before
            if (lat <= 90 && lat >= test && lon <= 180 && lon >= test2) {
              latlon = slat + "," + slon
            }
          } catch {
            //If the latitude or longitude can't be parsed into a double we don't want to index the values
            case e: Exception => slat = ""; slon = ""
          }
        }
        //get sensitive values map
        val sensitiveMap = {
          if (shouldIncludeSensitiveValue(getValue("dataResourceUid", map))) {
            val osv = getValue("originalSensitiveValues", map, "{}")
            if (StringUtils.isNotEmpty(osv)) {
              try {
                val parsed = JSON.parseFull(osv)
                parsed.get.asInstanceOf[Map[String, String]]
              } catch {
                case _: Exception => Map[String, String]()
              }
            } else {
              Map[String, String]()
            }
          } else {
            Map[String, String]()
          }
        }
        val sconservation = getParsedValue("stateConservation", map)
        var stateCons = if (sconservation != "") sconservation.split(",")(0) else ""
        val rawStateCons = if (sconservation != "") {
          val sconversations = sconservation.split(",")
          if (sconversations.length > 1)
            sconversations(1)
          else
            ""
        } else ""

        if (stateCons == "null") stateCons = rawStateCons

        val cconservation = getParsedValue("countryConservation", map)
        var countryCons = if (cconservation != "") cconservation.split(",")(0) else ""
        val rawCountryCons = if (cconservation != "") {
          val cconservations = cconservation.split(",")
          if (cconservations.length > 1)
            cconservations(1)
          else
            ""
        } else ""

        if (countryCons == "null") countryCons = rawCountryCons

        val sensitive: String = {
          val dataGen = getParsedValue("dataGeneralizations", map)
          if (dataGen.contains("already generalised"))
            "alreadyGeneralised"
          else if (dataGen != "")
            "generalised"
          else
            ""
        }

        val outlierForLayerStr = getParsedValue("outlierForLayers", map)
        if (StringUtils.isNotEmpty(outlierForLayerStr)) {
          var max_idx: Integer = 0
          jsonArrayLoop(outlierForLayerStr, (item, idx) => {
            max_idx = idx
            addField(doc, "outlier_layer", item)
          })
          addField(doc, "outlier_layer_count", max_idx)
        }

        val sDupType = getParsedValue("duplicationType", map, "[]")
        if (StringUtils.isNotEmpty(sDupType))
          jsonArrayLoop(outlierForLayerStr, (item, idx) => {
            addField(doc, "duplicate_type", item)
          })

        //Only set the geospatially kosher field if there are coordinates supplied
        val geoKosher = if (slat == "" && slon == "") "" else getValue(FullRecordMapper.geospatialDecisionColumn, map, "")

        //val hasUserAss = map.getOrElse(FullRecordMapper.userQualityAssertionColumn, "")
        val userAssertionStatus: Int = getValue(FullRecordMapper.userAssertionStatusColumn, map, AssertionStatus.QA_NONE.toString).toInt
        val hasUserAss: String = userAssertionStatus match {
          case AssertionStatus.QA_NONE => ""
          case _ => userAssertionStatus.toString
        }

        val (subspeciesGuid, subspeciesName): (String, String) = {
          if (hasParsedValue("taxonRankID", map)) {
            try {
              if (java.lang.Integer.parseInt(getParsedValue("taxonRankID", map, "")) > 7000)
                (taxonConceptId, sciName)
              else
                ("", "")
            } catch {
              case _: Exception => ("", "")
            }
          } else {
            ("", "")
          }
        }

        val lastLoaded = DateParser.parseStringToDate(getValue(FullRecordMapper.alaModifiedColumn, map))

        val lastProcessed = DateParser.parseStringToDate(getParsedValue(FullRecordMapper.alaModifiedColumn, map))

        val lastUserAssertion = DateParser.parseStringToDate(getValue(FullRecordMapper.lastUserAssertionDateColumn, map, ""))

        val firstLoadDate = DateParser.parseStringToDate(getValue("firstLoaded", map))

        val loanDate = DateParser.parseStringToDate(getValue("loanDate", map, ""))

        val loanReturnDate = DateParser.parseStringToDate(getValue("loanReturnDate", map, ""))

        val dateIdentified = DateParser.parseStringToDate(getParsedValue("dateIdentified", map))

        val modifiedDate = DateParser.parseStringToDate(getParsedValue("modified", map))

        var taxonIssue = getParsedValue("taxonomicIssue", map, "[]")
        if (!taxonIssue.startsWith("[")) {
          logger.warn("WARNING " + map.getOrElse("rowkey", "") + " does not have an updated taxonIssue: " + guid)
          taxonIssue = "[]"
        } else if (StringUtils.isNotEmpty(taxonIssue) && taxonIssue.length() > 2) {
          jsonArrayLoop(sdatahubs, (item, idx) => {
            addField(doc, "taxonomic_issue", item)
          })
        }

        val infoWith = getParsedValue("informationWithheld", map)
        val pest_tmp = if (infoWith.contains("\t")) infoWith.substring(0, infoWith.indexOf("\t")) else ""

        var distanceOutsideExpertRange = getParsedValue("distanceOutsideExpertRange", map)
        //only want valid numbers
        try {
          distanceOutsideExpertRange.toDouble
        } catch {
          case e: Exception => distanceOutsideExpertRange = ""
        }

        var i: Integer = 0
        addField(doc, header(i), getValue("uuid", map))
        i = i + 1
        addField(doc, header(i), getValue("rowkey", map))
        i = i + 1
        addField(doc, header(i), getValue("occurrenceID", map))
        i = i + 1
        //        if (dataHubUids != null)
        //          for (j <- dataHubUids)
        //            if (StringUtils.isNotEmpty(j)) addField(doc,header(i), j)
        i = i + 1
        addField(doc, header(i), getParsedValue("dataHub", map))
        i = i + 1
        addField(doc, header(i), getValue("dataProviderUid", map, checkParsed = true))
        i = i + 1
        addField(doc, header(i), getValue("dataProviderName", map, checkParsed = true))
        i = i + 1
        addField(doc, header(i), getValue("dataResourceUid", map, checkParsed = true))
        i = i + 1
        addField(doc, header(i), getValue("dataResourceName", map, checkParsed = true))
        i = i + 1
        addField(doc, header(i), getParsedValue("institutionUid", map))
        i = i + 1
        addField(doc, header(i), getValue("institutionCode", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("institutionName", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("collectionUid", map))
        i = i + 1
        addField(doc, header(i), getValue("collectionCode", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("collectionName", map))
        i = i + 1
        addField(doc, header(i), getValue("catalogNumber", map))
        i = i + 1
        addField(doc, header(i), taxonConceptId)
        i = i + 1
        addField(doc, header(i), if (eventDate != "") eventDate + "T00:00:00Z" else "")
        i = i + 1
        addField(doc, header(i), if (eventDateEnd != "") eventDateEnd + "T00:00:00Z" else "")
        i = i + 1
        addField(doc, header(i), occurrenceYear)
        i = i + 1
        addField(doc, header(i), occurrenceDecade)
        i = i + 1
        addField(doc, header(i), sciName)
        i = i + 1
        addField(doc, header(i), vernacularName)
        i = i + 1
        addField(doc, header(i), sciName + "|" + taxonConceptId + "|" + vernacularName + "|" + kingdom + "|" + family)
        i = i + 1
        addField(doc, header(i), vernacularName + "|" + sciName + "|" + taxonConceptId + "|" + vernacularName + "|" + kingdom + "|" + family)
        i = i + 1
        addField(doc, header(i), getParsedValue("taxonRank", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("taxonRankID", map))
        i = i + 1
        addField(doc, header(i), getRawScientificName(map))
        i = i + 1
        addField(doc, header(i), getValue("vernacularName", map))
        i = i + 1
        //if (!images.isEmpty && images(0) != "") "Multimedia" else "None"
        //        if (multimedia != null)
        //          for (j <- multimedia)
        //            if (StringUtils.isNotEmpty(j)) addField(doc,header(i), j)
        i = i + 1
        //        addField(doc,header(i), if (!images.isEmpty) images(0) else "")
        i = i + 1
        //        if (images != null)
        //          for (j <- images)
        //            if (StringUtils.isNotEmpty(j)) addField(doc,header(i), j)
        i = i + 1
        //        if (speciesGroup != null && speciesGroup.length > 0)
        //          for (j <- speciesGroup)
        //            if (StringUtils.isNotEmpty(j)) addField(doc,header(i), j)
        i = i + 1
        addField(doc, header(i), getValue("countryCode", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("country", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("left", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("right", map))
        i = i + 1
        addField(doc, header(i), kingdom)
        i = i + 1
        addField(doc, header(i), getParsedValue("phylum", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("classs", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("order", map))
        i = i + 1
        addField(doc, header(i), family)
        i = i + 1
        addField(doc, header(i), getParsedValue("genus", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("genusID", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("species", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("speciesID", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("stateProvince", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("lga", map))
        i = i + 1
        addField(doc, header(i), slat)
        i = i + 1
        addField(doc, header(i), slon)
        i = i + 1
        addField(doc, header(i), latlon)
        i = i + 1
        addField(doc, header(i), getLatLongString(lat, lon, "#"))
        i = i + 1
        addField(doc, header(i), getLatLongString(lat, lon, "#.#"))
        i = i + 1
        addField(doc, header(i), getLatLongString(lat, lon, "#.##"))
        i = i + 1
        addField(doc, header(i), getLatLongStringStep(lat, lon, "#.##", 0.02))
        i = i + 1
        addField(doc, header(i), getLatLongString(lat, lon, "#.###"))
        i = i + 1
        addField(doc, header(i), getLatLongString(lat, lon, "#.####"))
        i = i + 1
        addField(doc, header(i), getParsedValue("year", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("month", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("basisOfRecord", map))
        i = i + 1
        addField(doc, header(i), getValue("basisOfRecord", map))
        i = i + 1
        addField(doc, header(i), getValue("typeStatus", map))
        i = i + 1
        addField(doc, header(i), getValue("typeStatus", map))
        i = i + 1
        addField(doc, header(i), getValue(FullRecordMapper.taxonomicDecisionColumn, map))
        i = i + 1
        addField(doc, header(i), geoKosher)
        i = i + 1
        //NC 2013-05-23: Assertions are now values, failed, passed and untested these will be handled separately
        addField(doc, header(i), getValue("locationRemarks", map))
        i = i + 1
        addField(doc, header(i), getValue("occurrenceRemarks", map))
        i = i + 1
        addField(doc, header(i), hasUserAss)
        i = i + 1
        //  userAssertionStatu
        addField(doc, header(i), getValue("recordedBy", map))
        i = i + 1
        addField(doc, header(i), stateCons)
        i = i + 1 //stat
        addField(doc, header(i), rawStateCons)
        i = i + 1
        addField(doc, header(i), countryCons)
        i = i + 1
        addField(doc, header(i), rawCountryCons)
        i = i + 1
        addField(doc, header(i), sensitive)
        i = i + 1
        addField(doc, header(i), getParsedIntValue("coordinateUncertaintyInMeters", map))
        i = i + 1
        addField(doc, header(i), getValue("userId", map, ""))
        i = i + 1
        addField(doc, header(i), getValue("userId", map, ""))
        i = i + 1
        addField(doc, header(i), getParsedValue("provenance", map))
        i = i + 1
        addField(doc, header(i), subspeciesGuid)
        i = i + 1
        addField(doc, header(i), subspeciesName)
        i = i + 1
        //interactions (added above)
        i = i + 1
        addField(doc, header(i), if (lastUserAssertion.isEmpty) "" else DateFormatUtils.format(lastUserAssertion.get, "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        i = i + 1
        addField(doc, header(i), if (lastLoaded.isEmpty) "2010-11-1T00:00:00Z" else DateFormatUtils.format(lastLoaded.get, "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        i = i + 1
        addField(doc, header(i), if (lastProcessed.isEmpty) "" else DateFormatUtils.format(lastProcessed.get, "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        i = i + 1
        addField(doc, header(i), if (modifiedDate.isEmpty) "" else DateFormatUtils.format(modifiedDate.get, "yyy-MM-dd'T'HH:mm:ss'Z'"))
        i = i + 1
        for (v1 <- getParsedValue("establishmentMeans", map).split("; "))
          if (StringUtils.isNotEmpty(v1)) addField(doc, header(i), v1)
        i = i + 1
        addField(doc, header(i), map.getOrElse("loanSequenceNumber", ""))
        i = i + 1
        addField(doc, header(i), map.getOrElse("loanIdentifier", ""))
        i = i + 1
        addField(doc, header(i), map.getOrElse("loanDestination", ""))
        i = i + 1
        addField(doc, header(i), map.getOrElse("loanForBotanist", ""))
        i = i + 1
        addField(doc, header(i), if (loanDate.isEmpty) "" else DateFormatUtils.format(loanDate.get, "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        i = i + 1
        addField(doc, header(i), if (loanReturnDate.isEmpty) "" else DateFormatUtils.format(loanReturnDate.get, "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        i = i + 1
        addField(doc, header(i), getValue("originalNameUsage", map, getValue("typifiedName", map)))
        i = i + 1
        for (v2 <- map.getOrElse("duplicates", "").split('|'))
          if (StringUtils.isNotEmpty(v2)) addField(doc, header(i), v2)
        i = i + 1
        addField(doc, header(i), getValue("recordNumber", map))
        i = i + 1
        addField(doc, header(i), if (firstLoadDate.isEmpty) "" else DateFormatUtils.format(firstLoadDate.get, "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        i = i + 1
        addField(doc, header(i), getParsedValue("nameMatchMetric", map))
        i = i + 1
        addField(doc, header(i), map.getOrElse("phenology", ""))
        i = i + 1 //TODO make this a controlled vocab that gets mapped during processing...
        //        if (outlierForLayers != null)
        //          for (j <- outlierForLayers)
        //            if (StringUtils.isNotEmpty(j)) addField(doc,header(i), j)
        i = i + 1
        //        addField(doc,header(i), outlierForLayers.length.toString)
        i = i + 1
        //        if (taxonIssueArray != null)
        //          for (j <- taxonIssueArray)
        //            if (StringUtils.isNotEmpty(j)) addField(doc,header(i), j)
        i = i + 1
        addField(doc, header(i), getValue("identificationQualifier", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("identificationQualifier", map))
        i = i + 1
        //        if (habitats != null)
        //          for (j <- habitats)
        //            if (StringUtils.isNotEmpty(j)) addField(doc,header(i), j)
        i = i + 1
        addField(doc, header(i), getValue("identifiedBy", map))
        i = i + 1
        addField(doc, header(i), if (dateIdentified.isEmpty) "" else DateFormatUtils.format(dateIdentified.get, "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        i = i + 1
        addField(doc, header(i), sensitiveMap.getOrElse("decimalLongitude", ""))
        i = i + 1
        addField(doc, header(i), sensitiveMap.getOrElse("decimalLatitude", ""))
        i = i + 1
        addField(doc, header(i), pest_tmp)
        i = i + 1
        for (v1 <- getParsedValue("recordedBy", map).split('|'))
          if (StringUtils.isNotEmpty(v1)) addField(doc, header(i), v1)
        i = i + 1
        addField(doc, header(i), getParsedValue("duplicationStatus", map))
        i = i + 1
        for (v1 <- getParsedValue("associatedOccurrences", map).split('|'))
          if (StringUtils.isNotEmpty(v1)) addField(doc, header(i), v1)
        i = i + 1
        //        if (dupTypes != null)
        //          for (j <- dupTypes)
        //            if (StringUtils.isNotEmpty(j)) addField(doc,header(i), j)
        i = i + 1
        addField(doc, header(i), getParsedValue("coordinateUncertaintyInMeters", sensitiveMap))
        i = i + 1
        addField(doc, header(i), distanceOutsideExpertRange)
        i = i + 1
        addField(doc, header(i), getParsedValue("verbatimElevation", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("minimumElevationInMeters", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("maximumElevationInMeters", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("verbatimDepth", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("minimumDepthInMeters", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("maximumDepthInMeters", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("nameParseType", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("occurrenceStatus", map))
        i = i + 1
        addField(doc, header(i), getValue("occurrenceDetails", map))
        i = i + 1
        addField(doc, header(i), getValue("photographer", map))
        i = i + 1
        addField(doc, header(i), getValue("rights", map))
        i = i + 1
        addField(doc, header(i), getValue("georeferenceVerificationStatus", map))
        i = i + 1
        addField(doc, header(i), getValue("occurrenceStatus", map))
        i = i + 1
        addField(doc, header(i), getValue("locality", map))
        i = i + 1
        addField(doc, header(i), getValue("decimalLatitude", map))
        i = i + 1
        addField(doc, header(i), getValue("decimalLongitude", map))
        i = i + 1
        addField(doc, header(i), getValue("geodeticDatum", map))
        i = i + 1
        addField(doc, header(i), getValue("sex", map))
        i = i + 1
        addField(doc, header(i), getValue("locality", sensitiveMap))
        i = i + 1
        addField(doc, header(i), getValue("eventID", map))
        i = i + 1
        addField(doc, header(i), getValue("locationID", map))
        i = i + 1
        addField(doc, header(i), getValue("datasetName", map))
        i = i + 1
        addField(doc, header(i), getValue("reproductiveCondition", map))
        i = i + 1
        addField(doc, header(i), getParsedValue("license", map))
        i = i + 1
        addField(doc, header(i), getValue("individualCount", map))
        i = i + 1
        addField(doc, header(i), getValue("datePrecision", map))
        i = i + 1

        Config.additionalFieldsToIndex.foreach(field => {
          addField(doc, header(i), getValue(field, map))
          i = i + 1
        })
      }
    } catch {
      case e: Exception => logger.error(e.getMessage, e); throw e
    }
  }

  def writeOccIndexArrayToDoc(doc: DocBuilder, guid: String, array: GettableData) = {
    try {
      //get the lat lon values so that we can determine all the point values
      val deleted = getArrayValue(columnOrder.deletedColumn, array)
      //only add it to the index is it is not deleted & not a blank record
      if (!"true".equals(deleted)) {
        addFields(doc, array)
      }
    } catch {
      case e: Exception => logger.error(e.getMessage, e); throw e
    }
  }

  lazy val (
    array_header_idx, array_header_parsed_idx) = {
    val actual = new Array[Integer](headerAttributes.length)
    val parsed = new Array[Integer](headerAttributes.length)

    (actual, parsed)
  }

  lazy val (array_header_idx_fix, array_header_parsed_idx_fix) = {
    val actual = new Array[Integer](headerAttributesFix.length)
    val parsed = new Array[Integer](headerAttributesFix.length)

    (actual, parsed)
  }

  def addFields(doc: DocBuilder, array: GettableData): Unit = {

    //standard field copy
    headerAttributes.indices.foreach { i =>
      val h = headerAttributes(i)
      var value: String = {
        if (h._4 == 3) {
          getArrayValue(array_header_parsed_idx(i), array)
        } else if (h._4 >= 0) {
          val v = getArrayValue(array_header_idx(i), array)
          if (StringUtils.isEmpty(v) && h._4 == 0) {
            getArrayValue(array_header_parsed_idx(i), array)
          } else {
            v
          }
        } else {
          ""
        }
      }

      if (h._3 == 0) {
        try {
          val date = DateParser.parseStringToDate(value)
          if (StringUtils.isNotEmpty(date.toString)) value = date + "T00:00:00Z"
          else value = ""
        } catch {
          case _: Exception => value = ""
        }
      }

      if (StringUtils.isNotEmpty(value)) {
        if (h._3 == 4) {
          jsonArrayLoop(value, h._2, doc)
        } else if (h._3 == -1) {
          addField(doc, h._2, value)
        }
      }
    }

    //standard field copy fix. Excludes generalised records
    val dataGen = getArrayValue(columnOrder.dataGeneralizationsP, array)
    if (StringUtils.isEmpty(dataGen)) {
      for (i <- headerAttributesFix.indices) {
        val h = headerAttributesFix(i)
        var value: String = {
          if (h._4 == 3) {
            getArrayValue(array_header_parsed_idx_fix(i), array)
          } else if (h._4 >= 0) {
            val v = getArrayValue(array_header_idx_fix(i), array)
            if (StringUtils.isEmpty(v) && h._4 == 0) {
              getArrayValue(array_header_parsed_idx_fix(i), array)
            } else {
              v
            }
          } else {
            ""
          }
        }

        if (h._3 == 0) {
          try {
            val date = DateParser.parseStringToDate(value)
            if (StringUtils.isNotEmpty(date.toString)) value = date + "T00:00:00Z"
            else value = ""
          } catch {
            case _: Exception => value = ""
          }
        }

        if (StringUtils.isNotEmpty(value)) {
          if (h._3 == 4) {
            jsonArrayLoop(value, h._2, doc)
          } else if (h._3 == -1) {
            addField(doc, h._2, value)
          }
        }
      }
    }

    //latitude,longitude related fields
    var slat = getArrayValue(columnOrder.decimalLatitudeP, array)
    var slon = getArrayValue(columnOrder.decimalLongitudeP, array)
    if (StringUtils.isNotEmpty(slat) && StringUtils.isNotEmpty(slon)) {
      var latlon = ""
      var lat = java.lang.Double.NaN
      var lon = java.lang.Double.NaN
      try {
        lat = java.lang.Double.parseDouble(slat)
        lon = java.lang.Double.parseDouble(slon)
        val test = -90D
        val test2 = -180D
        //ensure that the lat longs are in the required range before
        if (lat <= 90 && lat >= test && lon <= 180 && lon >= test2) {
          latlon = slat + "," + slon
        }

        addField(doc, "lat_long", latlon)
        addField(doc, "point-1", getLatLongString(lat, lon, "#"))
        addField(doc, "point-0.1", getLatLongString(lat, lon, "#.#"))
        addField(doc, "point-0.01", getLatLongString(lat, lon, "#.##"))
        addField(doc, "point-0.02", getLatLongStringStep(lat, lon, "#.##", 0.02))
        addField(doc, "point-0.001", getLatLongString(lat, lon, "#.###"))
        addField(doc, "point-0.0001", getLatLongString(lat, lon, "#.####"))

        addField(doc, "geospatial_kosher", getArrayValue(columnOrder.geospatialDecisionColumn, array))
      } catch {
        //If the latitude or longitude can't be parsed into a double we don't want to index the values
        case e: Exception => slat = ""; slon = ""
      }
    }

    //images
    val simages = getArrayValue(columnOrder.images, array)
    if (StringUtils.isNotEmpty(simages)) {
      jsonArrayLoop(simages, (item, idx) => {
        if (idx == 0)
          addField(doc, "image_url", item)
        addField(doc, "all_image_url", item)
      })
    }

    //multimedia
    val s = getArrayValue(columnOrder.sounds, array)
    val v = getArrayValue(columnOrder.videos, array)
    if (simages.length() > 3) addField(doc, "multimedia", "Image")
    if (s.length() > 3) addField(doc, "multimedia", "Sound")
    if (v.length() > 3) addField(doc, "multimedia", "Video")

    //outlier
    val outlierForLayerStr = getArrayValue(columnOrder.outlierForLayersP, array)
    if (StringUtils.isNotEmpty(outlierForLayerStr)) {
      var max_idx: Integer = 0
      jsonArrayLoop(outlierForLayerStr, (item, idx) => {
        max_idx = idx
        addField(doc, "outlier_layer", item)
      })
      addField(doc, "outlier_layer_count", max_idx)
    }

    //decade
    var occurrenceYear = getArrayValue(columnOrder.yearP, array)
    if (occurrenceYear.length == 4) {
      occurrenceYear += "-01-01T00:00:00Z"
      addField(doc, "occurrence_year", occurrenceYear)
      addField(doc, "occurrence_decade_i", occurrenceYear.substring(0, 3) + "0")
    }

    //names_and_lsid, common_name_and_lsid
    val sciName = getArrayValue(columnOrder.scientificNameP, array)
    val taxonConceptId = getArrayValue(columnOrder.taxonConceptIDP, array)
    val vernacularName = getArrayValue(columnOrder.vernacularNameP, array).trim
    val kingdom = getArrayValue(columnOrder.kingdomP, array)
    val family = getArrayValue(columnOrder.familyP, array)
    addField(doc, "names_and_lsid", sciName + "|" + taxonConceptId + "|" + vernacularName + "|" + kingdom + "|" + family)
    addField(doc, "common_name_and_lsid", vernacularName + "|" + sciName + "|" + taxonConceptId + "|" + vernacularName + "|" + kingdom + "|" + family)

    //conservation
    val sconservation = getArrayValue(columnOrder.stateConservationP, array)
    if (StringUtils.isNotEmpty(sconservation)) {
      val split = sconservation.split(",")
      addField(doc, "state_conservation", split(0))
      if (split.length > 1)
        addField(doc, "raw_state_conservation", split(1))
    }
    val cconservation = getArrayValue(columnOrder.countryConservationP, array)
    if (StringUtils.isNotEmpty(cconservation)) {
      val split = cconservation.split(",")
      addField(doc, "country_conservation", split(0))
      if (split.length > 1)
        addField(doc, "raw_country_conservation", split(1))
    }

    //user_assertions
    val userAssertionStatus = getArrayValue(columnOrder.userAssertionStatusColumn, array)
    if (StringUtils.isNotEmpty(userAssertionStatus) && userAssertionStatus.equals(AssertionStatus.QA_NONE.toString)) {
      try {
        addField(doc, "user_assertions", userAssertionStatus.toInt.toString)
      } catch {
        case _: Exception => {}
      }
    }

    //subspecies fields
    val taxonRankIDP = getArrayValue(columnOrder.taxonRankIDP, array)
    if (StringUtils.isNotEmpty(taxonRankIDP)) {
      try {
        if (taxonRankIDP.toInt > 7000) {
          addField(doc, "subspecies_guid", taxonConceptId)
          addField(doc, "subspecies_name", sciName)
        }
      } catch {
        case _: Exception => {}
      }
    }

    //pest_flag
    val informationWithheldP = getArrayValue(columnOrder.informationWithheldP, array)
    if (StringUtils.isNotEmpty(informationWithheldP)) {
      if (informationWithheldP.contains("\t"))
        addField(doc, "pest_flag", informationWithheldP.substring(0, informationWithheldP.indexOf("\t")))

    }

    //sensitive fields
    //val dataGen = getArrayValue(columnOrder.dataGeneralizationsP, array)
    if (StringUtils.isNotEmpty(dataGen)) {
      if (dataGen.contains("already generalised")) {
        addField(doc, "sensitive", "alreadyGeneralised")
      } else if (dataGen != "") {
        addField(doc, "sensitive", "generalised")
      }
    }

    //sensitive values map
    val dataResourceUid = getArrayValue(columnOrder.dataResourceUid, array)
    if (StringUtils.isNotEmpty(dataResourceUid) && shouldIncludeSensitiveValue(dataResourceUid)) {
      val osv = getArrayValue(columnOrder.originalSensitiveValues, array, "")
      if (StringUtils.isNotEmpty(osv)) {
        try {
          val parsed = JSON.parseFull(osv).get.asInstanceOf[Map[String, String]]

          var v = String.valueOf(parsed.getOrElse("decimalLatitude", ""))
          if ("null" != v) addField(doc, "sensitive_latitude", v)
          v = String.valueOf(parsed.getOrElse("decimalLongitude", ""))
          if ("null" != v) addField(doc, "sensitive_longitude", v)
          addField(doc, "sensitive_coordinate_uncertainty", String.valueOf(parsed.getOrElse("coordinateUncertaintyInMeters" + Config.persistenceManager.fieldDelimiter + "p", "")))
          addField(doc, "sensitive_locality", String.valueOf(parsed.getOrElse("locality", "")))
        } catch {
          case _: Exception => Map[String, String]()
        }
      }
    }
    //all other values when not sensitive
    if (StringUtils.isEmpty(dataGen)) {
      for (i <- 0 until columnOrder.length.toInt) {
        if (!columnOrder.isUsed(i)) {
          addField(doc, columnOrder.columnNames(i), array.getString(i))
        }
      }
    }
  }

  def addField(doc: DocBuilder, field: String, value: Object) {
    if (value != null) {
      if (value.toString.length > 0) {
        doc.addField(field, value)
      }
    }
  }

  /**
    * Only loops over JSON arrays of strings that do not have formatted spacing
    *
    * @param jsonString
    * @param proc
    */
  def jsonArrayLoop(jsonString: String, proc: (String, Integer) => Unit) {

    if (StringUtils.isNotEmpty(jsonString)) {

      var start: Integer = jsonString.indexOf("\"") + 1
      var endOfCommaQuote: Integer = jsonString.indexOf("\",\"", start + 1)
      var endQuote: Integer = jsonString.indexOf("\"", start + 1)
      var pos: Integer = 0

      while (endOfCommaQuote > 0 || endQuote > 0) {
        if (endOfCommaQuote > 0) {
          proc(jsonString.substring(start, endOfCommaQuote), pos)
          start = endOfCommaQuote + 3
        } else {
          proc(jsonString.substring(start, endQuote), pos)
          start = endQuote + 3
        }
        endOfCommaQuote = jsonString.indexOf("\",\"", start + 1)
        endQuote = jsonString.indexOf("\"", start + 1)
        pos += 1
      }
    }
  }

  def jsonArrayLoop(jsonString: String, field: String, doc: DocBuilder) {
    jsonArrayLoop(jsonString, (item, idx) =>
      addField(doc, field, item)
    )
  }
}

/**
 * An class for handling a generic/common index fields
 *
 * Not in use yet.
 */
case class IndexField(fieldName: String, dataType: String, sourceField: String, multi: Boolean = false,
                      storeAsArray: Boolean = false, extraField: Option[String] = None, isMiscProperty: Boolean = false)

object IndexFields {

  val logger = LoggerFactory.getLogger("IndexFields")

  val fieldList = loadFromFile

  val indexFieldMap = fieldList.map(indexField => {
    (indexField.fieldName -> indexField.sourceField)
  }).toMap

  val storeFieldMap = fieldList.map(indexField => {
    (indexField.sourceField -> indexField.fieldName)
  }).toMap

  val storeMiscFields = fieldList collect {
    case value if value.isMiscProperty => value.sourceField
  }

  def loadFromFile() = {

    val overrideFile = new File("/data/biocache/config/indexFields.txt")
    val indexFieldSource = if (overrideFile.exists) {
      //if external file exists, use this
      logger.info("Reading vocab file: " + overrideFile.getAbsolutePath)
      scala.io.Source.fromFile(overrideFile, "utf-8")
    } else {
      //else use the file shipped with jar
      logger.info("Reading internal /indexFields.txt file")
      scala.io.Source.fromURL(getClass.getResource("/indexFields.txt"), "utf-8")
    }

    indexFieldSource.getLines.toList.collect {
      case row if !row.startsWith("#") && row.split("\t").length == 7 => {
        val values = row.split("\t")
        new IndexField(values(0),
          values(1),
          values(2),
          "T" == values(3),
          "T" == values(4),
          if (values(5).size > 0) Some(values(5)) else None, "T" == values(6)
        )
      }
    }
  }
}