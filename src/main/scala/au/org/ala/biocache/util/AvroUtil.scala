package au.org.ala.biocache.util

import java.io.File

import au.org.ala.biocache.Config
import au.org.ala.biocache.load.FullRecordMapper
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{GenericRecord, GenericDatumWriter}
import org.apache.commons.lang3.StringUtils

/**
  * Created by mar759 on 4/04/2016.
  */
object AvroUtil {

  /**
    * The header values for the CSV file.
    */
  val singleFields = List("id", "row_key", "occurrence_id", "data_hub_uid", "data_hub", "data_provider_uid",
    "data_provider", "data_resource_uid",
    "data_resource", "institution_uid", "institution_code", "institution_name",
    "collection_uid", "collection_code", "collection_name", "catalogue_number",
    "taxon_concept_lsid", "occurrence_date", "occurrence_date_end_dt", "occurrence_year", "occurrence_decade_i",
    "taxon_name", "common_name", "names_and_lsid", "common_name_and_lsid",
    "rank", "rank_id", "raw_taxon_name", "raw_common_name", "multimedia", "image_url", "all_image_url",
    "species_group", "country_code", "country", "lft", "rgt", "kingdom", "phylum", "class", "order",
    "family", "genus", "genus_guid", "species", "species_guid", "state", "places", "latitude", "longitude",
    "lat_long", "point_1", "point_0_1", "point_0_01", "point_0_02", "point_0_001", "point_0_0001",
    "year", "month", "basis_of_record", "raw_basis_of_record", "type_status",
    "raw_type_status", "taxonomic_kosher", "geospatial_kosher",  "location_remarks",
    "occurrence_remarks", "user_assertions", "collector", "state_conservation", "raw_state_conservation", "country_conservation", "raw_country_conservation",
    "sensitive", "coordinate_uncertainty", "user_id", "alau_user_id", "provenance", "subspecies_guid", "subspecies_name", "interaction", "last_assertion_date",
    "last_load_date", "last_processed_date", "modified_date", "establishment_means", "loan_number", "loan_identifier", "loan_destination",
    "loan_botanist", "loan_date", "loan_return_date", "original_name_usage", "duplicate_inst", "record_number", "first_loaded_date", "name_match_metric",
    "life_stage", "outlier_layer", "outlier_layer_count", "taxonomic_issue", "raw_identification_qualifier", "species_habitats",
    "identified_by", "identified_date", "sensitive_longitude", "sensitive_latitude", "pest_flag_s", "collectors", "duplicate_status", "duplicate_record",
    "duplicate_type", "sensitive_coordinate_uncertainty", "distance_outside_expert_range", "elevation_d", "min_elevation_d", "max_elevation_d",
    "depth_d", "min_depth_d", "max_depth_d", "name_parse_type_s","occurrence_status_s", "occurrence_details", "photographer_s", "rights",
    "raw_geo_validation_status_s", "raw_occurrence_status_s", "raw_locality","raw_latitude","raw_longitude","raw_datum","raw_sex",
    "sensitive_locality", "event_id", "location_id", "dataset_name", "reproductive_condition_s") ::: Config.additionalFieldsToIndex

  val arrDefaultMiscFields = if (Config.extraMiscFields == null) {
    Array[String]()
  } else {
    Config.extraMiscFields.split(",")
  }

  /**
    * All the fields to index in one large seq
    *
    */
  val csvHeader =
    singleFields :::
      arrDefaultMiscFields.toList :::
      List(
        FullRecordMapper.qualityAssertionColumn,
        FullRecordMapper.miscPropertiesColumn,
        "assertions_passed",
        "assertions_missing",
        "assertions",
        "assertions_unchecked",
        "system_assertions",
        "species_list_uid",
        "assertion_user_id",
        "query_assertion_uuid",
        "query_assertion_type_s",
        "suitable_modelling",
        "species_subgroup"
      ) :::
      Config.fieldsToSample().toList

  val multivaluedFields = List(
    "data_hub",
    "data_hub_uid",
    "assertions_passed",
    "assertions_missing",
    "assertions",
    "assertions_unchecked",
    "system_assertions",
    "species_list_uid",
    "assertion_user_id",
    "query_assertion_uuid",
    "query_assertion_type_s",
    "suitable_modelling",
    "species_subgroup",
    "species_group",
    "multimedia",
    "all_image_url",
    "taxonomic_issue",
    "geospatial_issue",
    "temporal_issue",
    "interaction",
    "outlier_layer",
    "establishment_means",
    "duplicate_inst",
    "species_habitats",
    "duplicate_record",
    "duplicate_type",
    "collectors"
  )

  def getAvroWriter(outputFilePath:String) : DataFileWriter[GenericRecord] = {

    val outputFile = new File(outputFilePath)
    val datumWriter = new GenericDatumWriter[GenericRecord](getAvroSchemaForIndex)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    dataFileWriter.setCodec(CodecFactory.snappyCodec())
    dataFileWriter.create(getAvroSchemaForIndex, outputFile)
    dataFileWriter
  }

  def getAvroSchemaForIndex : Schema = {

    val schemaJsonStart =
      """
        {
         "namespace": "au.org.ala.biocache",
         "type": "record",
         "name": "Record",
         "fields": ["""

    val fields = csvHeader.filter { StringUtils.isNotBlank(_) }.map { field =>
      if(multivaluedFields.contains(field)){
        "{\"name\": \"" + field + "\", \"type\": [{ \"type\": \"array\", \"items\": \"string\"}, \"null\"] }"
      } else {
        "{\"name\": \"" + field + "\", \"type\": [\"null\", \"string\"], \"default\" : null}"
      }
    }.mkString(",")

    val schemaJsonEnd = "]}"

    new Schema.Parser().parse(schemaJsonStart + fields + schemaJsonEnd)
  }
}
