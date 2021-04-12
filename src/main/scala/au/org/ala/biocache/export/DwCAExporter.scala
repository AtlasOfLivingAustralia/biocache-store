package au.org.ala.biocache.export

import java.io._
import java.util.zip._

import au.com.bytecode.opencsv.CSVWriter
import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.util.{Json, OptionParser}
import com.opencsv.{CSVReaderBuilder, RFC4180Parser}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory

import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.parsing.json.JSON
import scala.xml.Elem

/**
  * Companion object for the DwCAExporter class.
  */
object DwCAExporter extends Tool {

  def cmd = "export-dwca"

  def desc = "Export Darwin Core Archive for a data resource"

  val logger = LoggerFactory.getLogger("DwCAExporter")

  def main(args: Array[String]): Unit = {

    var dwcFieldsMap = mutable.LinkedHashMap(
      "rowkey" -> "",
      "abcdIdentificationQualifier" -> "http://rs.tdwg.org/abcd/terms/abcdIdentificationQualifier",
      "abcdIdentificationQualifierInsertionPoint" -> "http://rs.tdwg.org/abcd/terms/abcdIdentificationQualifierInsertionPoint",
      "abcdTypeStatus" -> "http://rs.tdwg.org/abcd/terms/abcdTypeStatus",
      "acceptedNameUsage" -> "http://rs.tdwg.org/dwc/terms/acceptedNameUsage",
      "acceptedNameUsageID" -> "http://rs.tdwg.org/dwc/terms/acceptedNameUsageID",
      "accessRights" -> "http://purl.org/dc/terms/accessRights",
//      Skip exportying associatedMedia as the images has already been exported in the DwcA image extension
//      "associatedMedia" -> "http://rs.tdwg.org/dwc/terms/associatedMedia",
      "associatedOccurrences" -> "http://rs.tdwg.org/dwc/terms/associatedOccurrences",
      "associatedReferences" -> "http://rs.tdwg.org/dwc/terms/associatedReferences",
      "associatedSequences" -> "http://rs.tdwg.org/dwc/terms/associatedSequences",
      "associatedTaxa" -> "http://rs.tdwg.org/dwc/terms/associatedTaxa",
      "basisOfRecord" -> "http://rs.tdwg.org/dwc/terms/basisOfRecord",
      "behavior" -> "http://rs.tdwg.org/dwc/terms/behavior",
      "bibliographicCitation" -> "http://purl.org/dc/terms/bibliographicCitation",
      "catalogNumber" -> "http://rs.tdwg.org/dwc/terms/catalogNumber",
      "class" -> "http://rs.tdwg.org/dwc/terms/class",
      "collectionCode" -> "http://rs.tdwg.org/dwc/terms/collectionCode",
      "collectionID" -> "http://rs.tdwg.org/dwc/terms/collectionID",
      "continent" -> "http://rs.tdwg.org/dwc/terms/continent",
      "coordinatePrecision" -> "http://rs.tdwg.org/dwc/terms/coordinatePrecision",
      "coordinateUncertaintyInMeters" -> "http://rs.tdwg.org/dwc/terms/coordinateUncertaintyInMeters",
      "country" -> "http://rs.tdwg.org/dwc/terms/country",
      "countryCode" -> "http://rs.tdwg.org/dwc/terms/countryCode",
      "county" -> "http://rs.tdwg.org/dwc/terms/county",
      "dataGeneralizations" -> "http://rs.tdwg.org/dwc/terms/dataGeneralizations",
      "datasetID" -> "http://rs.tdwg.org/dwc/terms/datasetID",
      "datasetName" -> "http://rs.tdwg.org/dwc/terms/datasetName",
      "dateIdentified" -> "http://rs.tdwg.org/dwc/terms/dateIdentified",
      "day" -> "http://rs.tdwg.org/dwc/terms/day",
      "decimalLatitude" -> "http://rs.tdwg.org/dwc/terms/decimalLatitude",
      "decimalLongitude" -> "http://rs.tdwg.org/dwc/terms/decimalLongitude",
      "disposition" -> "http://rs.tdwg.org/dwc/terms/disposition",
      "dynamicProperties" -> "http://rs.tdwg.org/dwc/terms/dynamicProperties",
      "easting" -> "http://rs.ala.org.au/terms/1.0/easting",
      "endDayOfYear" -> "http://rs.tdwg.org/dwc/terms/endDayOfYear",
      "establishmentMeans" -> "http://rs.tdwg.org/dwc/terms/establishmentMeans",
      "eventAttributes" -> "http://rs.tdwg.org/dwc/terms/eventAttributes",
      "eventDate" -> "http://rs.tdwg.org/dwc/terms/eventDate",
      "eventID" -> "http://rs.tdwg.org/dwc/terms/eventID",
      "eventRemarks" -> "http://rs.tdwg.org/dwc/terms/eventRemarks",
      "eventTime" -> "http://rs.tdwg.org/dwc/terms/eventTime",
      "family" -> "http://rs.tdwg.org/dwc/terms/family",
      "fieldNotes" -> "http://rs.tdwg.org/dwc/terms/fieldNotes",
      "fieldNumber" -> "http://rs.tdwg.org/dwc/terms/fieldNumber",
      "footprintSpatialFit" -> "http://rs.tdwg.org/dwc/terms/footprintSpatialFit",
      "footprintSRS" -> "http://rs.tdwg.org/dwc/terms/footprintSRS",
      "footprintWKT" -> "http://rs.tdwg.org/dwc/terms/footprintWKT",
      "genus" -> "http://rs.tdwg.org/dwc/terms/genus",
      "geodeticDatum" -> "http://rs.tdwg.org/dwc/terms/geodeticDatum",
      "georeferencedBy" -> "http://rs.tdwg.org/dwc/terms/georeferencedBy",
      "georeferencedDate" -> "http://rs.tdwg.org/dwc/terms/georeferencedDate",
      "georeferenceProtocol" -> "http://rs.tdwg.org/dwc/terms/georeferenceProtocol",
      "georeferenceRemarks" -> "http://rs.tdwg.org/dwc/terms/georeferenceRemarks",
      "georeferenceSources" -> "http://rs.tdwg.org/dwc/terms/georeferenceSources",
      "georeferenceVerificationStatus" -> "http://rs.tdwg.org/dwc/terms/georeferenceVerificationStatus",
      "habitat" -> "http://rs.tdwg.org/dwc/terms/habitat",
      "higherClassification" -> "http://rs.tdwg.org/dwc/terms/higherClassification",
      "higherGeography" -> "http://rs.tdwg.org/dwc/terms/higherGeography",
      "higherGeographyID" -> "http://rs.tdwg.org/dwc/terms/higherGeographyID",
      "identificationID" -> "http://rs.tdwg.org/dwc/terms/identificationID",
      "identificationQualifier" -> "http://rs.tdwg.org/dwc/terms/identificationQualifier",
      "identificationReferences" -> "http://rs.tdwg.org/dwc/terms/identificationReferences",
      "identificationRemarks" -> "http://rs.tdwg.org/dwc/terms/identificationRemarks",
      "identificationVerificationStatus" -> "http://rs.tdwg.org/dwc/terms/identificationVerificationStatus",
      "identifiedBy" -> "http://rs.tdwg.org/dwc/terms/identifiedBy",
      "identifierRole" -> "http://hiscom.chah.org.au/hispid/terms/identifierRole",
      "individualCount" -> "http://rs.tdwg.org/dwc/terms/individualCount",
      "informationWithheld" -> "http://rs.tdwg.org/dwc/terms/informationWithheld",
      "infraspecificEpithet" -> "http://rs.tdwg.org/dwc/terms/infraspecificEpithet",
      "institutionCode" -> "http://rs.tdwg.org/dwc/terms/institutionCode",
      "institutionID" -> "http://rs.tdwg.org/dwc/terms/institutionID",
      "island" -> "http://rs.tdwg.org/dwc/terms/island",
      "islandGroup" -> "http://rs.tdwg.org/dwc/terms/islandGroup",
      "kingdom" -> "http://rs.tdwg.org/dwc/terms/kingdom",
      "language" -> "http://purl.org/dc/terms/language",
      "license" -> "http://purl.org/dc/terms/license",
      "lifeStage" -> "http://rs.tdwg.org/dwc/terms/lifeStage",
      "loanDate" -> "http://data.ggbn.org/schemas/ggbn/terms/loanDate",
      "loanDestination" -> "http://data.ggbn.org/schemas/ggbn/terms/loanDestination",
      "loanIdentifier" -> "http://data.ggbn.org/schemas/ggbn/terms/loanIdentifier",
      "locality" -> "http://rs.tdwg.org/dwc/terms/locality",
      "locationAccordingTo" -> "http://rs.tdwg.org/dwc/terms/locationAccordingTo",
      "locationAttributes" -> "http://rs.tdwg.org/dwc/terms/locationAttributes",
      "locationID" -> "http://rs.tdwg.org/dwc/terms/locationID",
      "locationRemarks" -> "http://rs.tdwg.org/dwc/terms/locationRemarks",
      "maximumDepthInMeters" -> "http://rs.tdwg.org/dwc/terms/maximumDepthInMeters",
      "maximumDistanceAboveSurfaceInMeters" -> "http://rs.tdwg.org/dwc/terms/maximumDistanceAboveSurfaceInMeters",
      "maximumElevationInMeters" -> "http://rs.tdwg.org/dwc/terms/maximumElevationInMeters",
      "measurementAccuracy" -> "http://rs.tdwg.org/dwc/terms/measurementAccuracy",
      "measurementDeterminedBy" -> "http://rs.tdwg.org/dwc/terms/measurementDeterminedBy",
      "measurementDeterminedDate" -> "http://rs.tdwg.org/dwc/terms/measurementDeterminedDate",
      "measurementID" -> "http://rs.tdwg.org/dwc/terms/measurementID",
      "measurementMethod" -> "http://rs.tdwg.org/dwc/terms/measurementMethod",
      "measurementRemarks" -> "http://rs.tdwg.org/dwc/terms/measurementRemarks",
      "measurementType" -> "http://rs.tdwg.org/dwc/terms/measurementType",
      "measurementUnit" -> "http://rs.tdwg.org/dwc/terms/measurementUnit",
      "measurementValue" -> "http://rs.tdwg.org/dwc/terms/measurementValue",
      "minimumDepthInMeters" -> "http://rs.tdwg.org/dwc/terms/minimumDepthInMeters",
      "minimumDistanceAboveSurfaceInMeters" -> "http://rs.tdwg.org/dwc/terms/minimumDistanceAboveSurfaceInMeters",
      "minimumElevationInMeters" -> "http://rs.tdwg.org/dwc/terms/minimumElevationInMeters",
      "modified" -> "http://purl.org/dc/terms/modified",
      "month" -> "http://rs.tdwg.org/dwc/terms/month",
      "municipality" -> "http://rs.tdwg.org/dwc/terms/municipality",
      "nameAccordingTo" -> "http://rs.tdwg.org/dwc/terms/nameAccordingTo",
      "nameAccordingToID" -> "http://rs.tdwg.org/dwc/terms/nameAccordingToID",
      "namePublishedIn" -> "http://rs.tdwg.org/dwc/terms/namePublishedIn",
      "namePublishedInID" -> "http://rs.tdwg.org/dwc/terms/namePublishedInID",
      "namePublishedInYear" -> "http://rs.tdwg.org/dwc/terms/namePublishedInYear",
      "nomenclaturalCode" -> "http://rs.tdwg.org/dwc/terms/nomenclaturalCode",
      "nomenclaturalStatus" -> "http://rs.tdwg.org/dwc/terms/nomenclaturalStatus",
      "northing" -> "http://rs.ala.org.au/terms/1.0/northing",
      "occurrenceAttributes" -> "http://rs.tdwg.org/dwc/terms/occurrenceAttributes",
      "occurrenceID" -> "http://rs.tdwg.org/dwc/terms/occurrenceID",
      "occurrenceRemarks" -> "http://rs.tdwg.org/dwc/terms/occurrenceRemarks",
      "occurrenceStatus" -> "http://rs.tdwg.org/dwc/terms/occurrenceStatus",
      "order" -> "http://rs.tdwg.org/dwc/terms/order",
      "organismID" -> "http://rs.tdwg.org/dwc/terms/organismID",
      "organismQuantity" -> "http://rs.tdwg.org/dwc/terms/organismQuantity",
      "organismQuantityType" -> "http://rs.tdwg.org/dwc/terms/organismQuantityType",
      "originalNameUsage" -> "http://rs.tdwg.org/dwc/terms/originalNameUsage",
      "originalNameUsageID" -> "http://rs.tdwg.org/dwc/terms/originalNameUsageID",
      "otherCatalogNumbers" -> "http://rs.tdwg.org/dwc/terms/otherCatalogNumbers",
      "ownerInstitutionCode" -> "http://rs.tdwg.org/dwc/terms/ownerInstitutionCode",
      "parentNameUsage" -> "http://rs.tdwg.org/dwc/terms/parentNameUsage",
      "parentNameUsageID" -> "http://rs.tdwg.org/dwc/terms/parentNameUsageID",
      "phylum" -> "http://rs.tdwg.org/dwc/terms/phylum",
      "pointRadiusSpatialFit" -> "http://rs.tdwg.org/dwc/terms/pointRadiusSpatialFit",
      "preparations" -> "http://rs.tdwg.org/dwc/terms/preparations",
      "previousIdentifications" -> "http://rs.tdwg.org/dwc/terms/previousIdentifications",
      "photographer" -> "http://rs.ala.org.au/terms/1.0/photographer",
      "recordedBy" -> "http://rs.tdwg.org/dwc/terms/recordedBy",
      "recordNumber" -> "http://rs.tdwg.org/dwc/terms/recordNumber",
      "references" -> "http://purl.org/dc/terms/references",
      "relatedResourceID" -> "http://rs.tdwg.org/dwc/terms/relatedResourceID",
      "relationshipAccordingTo" -> "http://rs.tdwg.org/dwc/terms/relationshipAccordingTo",
      "relationshipEstablishedDate" -> "http://rs.tdwg.org/dwc/terms/relationshipEstablishedDate",
      "relationshipOfResource" -> "http://rs.tdwg.org/dwc/terms/relationshipOfResource",
      "relationshipRemarks" -> "http://rs.tdwg.org/dwc/terms/relationshipRemarks",
      "reproductiveCondition" -> "http://rs.tdwg.org/dwc/terms/reproductiveCondition",
      "resourceID" -> "http://rs.tdwg.org/dwc/terms/resourceID",
      "resourceRelationshipID" -> "http://rs.tdwg.org/dwc/terms/resourceRelationshipID",
      "rights" -> "http://purl.org/dc/terms/rights",
      "rightsHolder" -> "http://purl.org/dc/terms/rightsHolder",
      "samplingEffort" -> "http://rs.tdwg.org/dwc/terms/samplingEffort",
      "samplingProtocol" -> "http://rs.tdwg.org/dwc/terms/samplingProtocol",
      "scientificName" -> "http://rs.tdwg.org/dwc/terms/scientificName",
      "scientificNameAuthorship" -> "http://rs.tdwg.org/dwc/terms/scientificNameAuthorship",
      "scientificNameID" -> "http://rs.tdwg.org/dwc/terms/scientificNameID",
      "secondaryCollectors" -> "http://hiscom.chah.org.au/hispid/terms/secondaryCollectors",
      "sex" -> "http://rs.tdwg.org/dwc/terms/sex",
      "source" -> "http://purl.org/dc/terms/source",
      "species" -> "http://rs.ala.org.au/terms/1.0/species",
      "specificEpithet" -> "http://rs.tdwg.org/dwc/terms/specificEpithet",
      "startDayOfYear" -> "http://rs.tdwg.org/dwc/terms/startDayOfYear",
      "stateProvince" -> "http://rs.tdwg.org/dwc/terms/stateProvince",
      "subgenus" -> "http://rs.tdwg.org/dwc/terms/subgenus",
      "subfamily" -> "http://rs.ala.org.au/terms/1.0/subfamily",
      "subspecies" -> "http://rs.ala.org.au/terms/1.0/subspecies",
      "superfamily" -> "http://rs.ala.org.au/terms/1.0/superfamily",
      "taxonConceptID" -> "http://rs.tdwg.org/dwc/terms/taxonConceptID",
      "taxonID" -> "http://rs.tdwg.org/dwc/terms/taxonID",
      "taxonomicStatus" -> "http://rs.tdwg.org/dwc/terms/taxonomicStatus",
      "taxonRank" -> "http://rs.tdwg.org/dwc/terms/taxonRank",
      "taxonRemarks" -> "http://rs.tdwg.org/dwc/terms/taxonRemarks",
      "type" -> "http://purl.org/dc/terms/type",
      "typeStatus" -> "http://rs.tdwg.org/dwc/terms/typeStatus",
      "typifiedName" -> "http://rs.tdwg.org/abcd/terms/typifiedName",
      "userId" -> "http://rs.gbif.org/terms/1.0/recordedByID",
      "verbatimCoordinates" -> "http://rs.tdwg.org/dwc/terms/verbatimCoordinates",
      "verbatimCoordinateSystem" -> "http://rs.tdwg.org/dwc/terms/verbatimCoordinateSystem",
      "verbatimDepth" -> "http://rs.tdwg.org/dwc/terms/verbatimDepth",
      "verbatimElevation" -> "http://rs.tdwg.org/dwc/terms/verbatimElevation",
      "verbatimEventDate" -> "http://rs.tdwg.org/dwc/terms/verbatimEventDate",
      "verbatimLatitude" -> "http://rs.tdwg.org/dwc/terms/verbatimLatitude",
      "verbatimLocality" -> "http://rs.tdwg.org/dwc/terms/verbatimLocality",
      "verbatimLongitude" -> "http://rs.tdwg.org/dwc/terms/verbatimLongitude",
      "verbatimSRS" -> "http://rs.tdwg.org/dwc/terms/verbatimSRS",
      "verbatimTaxonRank" -> "http://rs.tdwg.org/dwc/terms/verbatimTaxonRank",
      "vernacularName" -> "http://rs.tdwg.org/dwc/terms/vernacularName",
      "waterBody" -> "http://rs.tdwg.org/dwc/terms/waterBody",
      "year" -> "http://rs.tdwg.org/dwc/terms/year",
      "zone" -> "http://rs.ala.org.au/terms/1.0/zone"
    )

    var resourceUid = ""
    var directory = ""
    var threads = 4
    var pageSize = 1000
    var addImagesToExisting = false
    var exportExtensions = false

    def writeCsvRow(resourceIDs: Seq[String], dataResource2OutputStreams: Map[String, Option[(ZipOutputStream, CSVWriter)]], map: Map[String, String]) = {
      val dr = map.getOrElse("dataResourceUid", "")
      val dateDeleted = map.getOrElse("dateDeleted", "")

      def extractOriginalSensitiveProperties: (Map[String, String], Map[String, String]) = {
        if (!map.getOrElse("originalSensitiveValues", "").isEmpty) {
          val json = JSON.parseFull(map.getOrElse("originalSensitiveValues", "")).get.asInstanceOf[Map[String, String]]
          var originalMiscProperties = new mutable.HashMap[String, String]()
          var originalProperties = new mutable.HashMap[String, String]()
          json.filterKeys((k) => if (!k.endsWith("_p")) true else false) foreach (originalTerm => {
            if (dwcFieldsMap.contains(originalTerm._1)) {
              originalProperties += (originalTerm._1 -> originalTerm._2)
            } else {
              originalMiscProperties += (originalTerm._1 -> originalTerm._2)
            }
          })
          (originalProperties.toMap, originalMiscProperties.toMap)
        } else
          (Map.empty[String, String], Map.empty[String, String])
      }

      if (!dr.isEmpty && resourceIDs.contains(dr) && dateDeleted.isEmpty) { // Record is not deleted
        val dataResourceMap = dataResource2OutputStreams.get(dr)
        if (!dataResourceMap.isEmpty && !dataResourceMap.get.isEmpty) {
          val (zop, csv) = dataResourceMap.get.get
          val (originalProperties, originalMiscProperties) = extractOriginalSensitiveProperties
          val resultMap = map.filter(_._2 != null).map({ (entry) =>
            entry._1 match {
              case "individualID" =>
                ("organismID", originalProperties.getOrElse(entry._1, entry._2))
              case "occurrenceDetails" =>
                ("references", originalProperties.getOrElse(entry._1, entry._2))
              case "class" =>
                // class field in dwca can include the combincation of the following fields if the prior field is empty : class, classs, and _class
                (entry._1, originalProperties.getOrElse(entry._1, if (!entry._2.isEmpty()) entry._2; else if (!map.getOrElse("classs", "").isEmpty) map.getOrElse("classs", ""); else map.getOrElse("_class", "")))
              case "identifiedBy" =>
                // it will include identifierBy field if the identifiedBy is empty
                (entry._1, originalProperties.getOrElse(entry._1, if (!entry._2.isEmpty()) entry._2; else map.getOrElse("identifierBy", "")))
              case "miscProperties" =>
                if (originalMiscProperties.isEmpty)
                  ("dynamicProperties", entry._2)
                else {
                  val miscProperties = JSON.parseFull(map.getOrElse("miscProperties", "")).get.asInstanceOf[Map[String, String]]
                  ("dynamicProperties", Json.toJSON(miscProperties ++ originalMiscProperties))
                }
              case _ =>
                (entry._1, originalProperties.getOrElse(entry._1, entry._2))
            }
          }).filter(term => dwcFieldsMap.contains(term._1))
          val row = (dwcFieldsMap).map((fieldMap) => cleanValue(resultMap.getOrElse(fieldMap._1, null))).toArray
          synchronized {
            csv.writeNext(
              row
            )
            csv.flush()
          }
        }
      }
    }

    val parser = new OptionParser(help) {
      arg("data-resource-uid", "Comma separated list of DRs or 'all' to generate for all",
        { v: String => resourceUid = v }
      )
      arg("directory-to-dump", "Directory to place the created archives",
        { v: String => directory = v }
      )
      opt("f", "fields", "Comma separated list of DwC fields to export (according to cassandra DB). Default is :" + dwcFieldsMap.keySet,
        { v: String =>
          val fields = v.split(",").toList
          dwcFieldsMap = dwcFieldsMap.filter((field) => fields.contains(field._1))
        }
      )
      intOpt("t", "thread", "The number of threads to use. Default is " + threads, { v: Int => threads = v })
      intOpt("ps", "pageSize", "The pageSize to use. Default is " + pageSize, { v: Int => pageSize = v })

      booleanOpt("export-extensions", "Export extensions too.", { v: Boolean => exportExtensions = v })

      booleanOpt("add-images-to-existing-only", "Add images to existing archives.", { v: Boolean => addImagesToExisting = v })
    }

    if (parser.parse(args)) {
      val dwcc = new DwCAExporter(dwcFieldsMap)

      if (addImagesToExisting) {
        dwcc.addImageExportsToArchives(directory)
      } else {
        try {

          val resourceIDs = if (resourceUid == "all") {
            getDataResourceUids
          } else {
            resourceUid.split(",").map(_.trim).toList
          }

          val dataResource2OutputStreams = resourceIDs.map { uid => (uid, dwcc.createOutputForCSV(directory, uid)) }.toMap
          Config.persistenceManager.pageOverSelect("occ", (key, map) => {
            synchronized {
              writeCsvRow(resourceIDs, dataResource2OutputStreams, map)
            }
            true
          }, pageSize, threads)
          //finish write of CSV to zip
          dataResource2OutputStreams.values.foreach { zopAndCsv =>
            if (!zopAndCsv.isEmpty) {
              zopAndCsv.get._1.flush()
              zopAndCsv.get._1.closeEntry()
              zopAndCsv.get._1.close()
            }
          }
          //add images
          if (exportExtensions) {
            dwcc.addImageExportsToArchives(directory)
          }
        } catch {
          case e: Exception => {
            logger.error(e.getMessage(), e)
            throw new RuntimeException(e)
          }
        }
      }
    }

    Config.persistenceManager.shutdown
  }

  def cleanValue(input: String) = if (input == null) "" else input.replaceAll("[\\t\\n\\r]", " ").trim

  // pattern to extract a data resource uid from a filter query , because the label show i18n value
  val dataResourcePattern = "(?:[\"]*)?(?:[a-z_]*_uid:\")([a-z0-9]*)(?:[\"]*)?".r

  def getDataResourceUids: Seq[String] = {
    val url = Config.biocacheServiceUrl + "/occurrences/search?q=*:*&facets=data_resource_uid&pageSize=0&flimit=-1"
    val jsonString = Source.fromURL(url).getLines.mkString
    val json = JSON.parseFull(jsonString).get.asInstanceOf[Map[String, String]]
    val results = json.get("facetResults").get.asInstanceOf[List[Map[String, String]]].head.get("fieldResult").get.asInstanceOf[List[Map[String, String]]]
    results.map { facet =>
      val fq = facet.get("fq").get
      parseFq(fq)
    }.filterNot(_.equals("Unknown"))
  }

  def parseFq(fq: String): String = fq match {
    case dataResourcePattern(dr) => dr
    case _ => "Unknown"
  }
}

/**
  * Class for creating a Darwin Core Archive from data in the biocache.
  *
  * TODO support for dwc fields in registry metadata. When not available use the default fields.
  */
class DwCAExporter(fieldList: mutable.LinkedHashMap[String, String]) {

  val logger = LoggerFactory.getLogger("DwCAExporter")

  def createOutputForCSV(directory: String, dataResource: String): Option[(ZipOutputStream, CSVWriter)] = {

    logger.info("Creating archive for " + dataResource)
    val zipFile = new java.io.File(
      directory +
        System.getProperty("file.separator") +
        dataResource +
        System.getProperty("file.separator") +
        dataResource +
        ".zip"
    )

    FileUtils.forceMkdir(zipFile.getParentFile)
    val zop = new ZipOutputStream(new FileOutputStream(zipFile))
    if (addEML(zop, dataResource)) {
      addMeta(zop, List())
      zop.putNextEntry(new ZipEntry("occurrence.csv"))
      val occWriter = new CSVWriter(new OutputStreamWriter(zop), ',', '"', "\r\n")
      Some((zop, occWriter))
    } else {
      //no EML implies that a DWC-A should not be generated.
      zop.close()
      FileUtils.deleteQuietly(zipFile)
      None
    }
  }

  def addEML(zop: ZipOutputStream, dr: String): Boolean = {
    try {
      zop.putNextEntry(new ZipEntry("eml.xml"))
      val content = Source.fromURL(Config.registryUrl + "/eml/" + dr).mkString
      zop.write(content.getBytes)
      zop.flush
      zop.closeEntry
      true
    } catch {
      case e: Exception =>
        logger.error("Problem retrieving metadata for EML: " + dr + ", " + e.getMessage())
        if (logger.isDebugEnabled) {
          logger.debug("Problem retrieving metadata for EML: " + dr + ", " + e.getMessage(), e)
        }
        false
    }
  }
  private def buildMetaXml(fieldsSeq: immutable.IndexedSeq[String], extensions: List[String]) = {
    val metaXml = <archive xmlns="http://rs.tdwg.org/dwc/text/" metadata="eml.xml">
      <core encoding="UTF-8" linesTerminatedBy="\r\n" fieldsTerminatedBy="," fieldsEnclosedBy="&quot;" ignoreHeaderLines="0" rowType="http://rs.tdwg.org/dwc/terms/Occurrence">
        <files>
          <location>occurrence.csv</location>
        </files>
        <id index="0"/>{fieldsSeq.zipWithIndex.map {
        case (field, index) =>
            <field index={index + 1 + ""} term={fieldList(fieldsSeq(index))}/>
      }}
      </core>{extensions.map {
        case "Multimedia" =>
          <extension encoding="UTF-8" linesTerminatedBy="\r\n" fieldsTerminatedBy="," fieldsEnclosedBy="&quot;" ignoreHeaderLines="0" rowType="http://rs.gbif.org/terms/1.0/Multimedia">
            <files>
              <location>image.csv</location>
            </files>
            <coreid index="0"/>
            <field index="0" term="id"/>
            <field index="1" term="http://purl.org/dc/terms/identifier"/>
            <field index="2" term="http://purl.org/dc/terms/creator"/>
            <field index="3" term="http://purl.org/dc/terms/created"/>
            <field index="4" term="http://purl.org/dc/terms/title"/>
            <field index="5" term="http://purl.org/dc/terms/format"/>
            <field index="6" term="http://purl.org/dc/terms/license"/>
            <field index="7" term="http://purl.org/dc/terms/rights"/>
            <field index="8" term="http://purl.org/dc/terms/rightsHolder"/>
            <field index="9" term="http://purl.org/dc/terms/references"/>
          </extension>
        case _ =>
      }}
    </archive>
    metaXml
  }

  def addMeta(zop: ZipOutputStream, extensions: List[String]) = {
    zop.putNextEntry(new ZipEntry("meta.xml"))
    val fieldsSeq = (fieldList - "dataResourceUid" - "classs" - "rowkey").keySet.toIndexedSeq
    val metaXml: Elem = buildMetaXml(fieldsSeq, extensions)
    //add the XML
    zop.write("""<?xml version="1.0"?>""".getBytes)
    zop.write("\n".getBytes)
    zop.write(metaXml.mkString("\n").getBytes)
    zop.flush
    zop.closeEntry
  }


  /**
    * Retrieves an archive from the image service and then appends contents to
    * existing created archives.
    *
    * @return
    */
  def addImageExportsToArchives(archivesPath: String) {

    if (Config.remoteMediaStoreUrl == "") return

    val workingDir = Config.tmpWorkDir + "/images-export"
    val workingDirSplitFiles = workingDir + "/split"
    val imagesExport = workingDir + "/images-export.csv.gz"

    //create working directories
    FileUtils.forceMkdir(new File(workingDirSplitFiles))

    //download the gzip from images.ala.org.au....
    logger.info("Downloading images archive extract....")
    downloadToFile(Config.remoteMediaStoreUrl + "/ws/exportCSV", imagesExport)
    logger.info("Downloaded images archive extract to " + imagesExport)

    //download the images export
    logger.info("Extracting Gzip....")
    extractGzip(imagesExport, workingDir + "/images-export.csv")

    //assume output is RFC4180 - i.e. no escape character is in use, and quotes are respected.
    val reader = new CSVReaderBuilder(new FileReader(workingDir + "/images-export.csv"))
      .withSkipLines(1)
      .withCSVParser(new RFC4180Parser())
      .build()

    var line = reader.readNext()

    var currentUid = ""
    var writer: CSVWriter = null
    val list = new ListBuffer[String]

    logger.info("Splitting into separate files to...." + workingDirSplitFiles)
    while (line != null) {

      val dataResourceUid = line(0)
      if (dataResourceUid != currentUid && StringUtils.isNotEmpty(dataResourceUid)) {
        if (writer != null) {
          writer.flush()
          writer.close()
          writer = null
        }
        currentUid = dataResourceUid
        list += dataResourceUid
        writer = new CSVWriter(new FileWriter(workingDirSplitFiles + "/" + dataResourceUid))
      }

      if (StringUtils.isNotEmpty(dataResourceUid)) {
        writer.writeNext(line.slice(1, line.length))
      }

      line = reader.readNext()
    }

    if (writer != null) {
      writer.flush()
      writer.close()
      writer = null
    }

    logger.info("Adding to existing archives..." + archivesPath)
    // add to the archives
    list.foreach { dataResourceUid =>

      //find the archive....
      val archivePath = archivesPath + "/" + dataResourceUid + "/" + dataResourceUid + ".zip"
      val archive = new File(archivePath)
      if (archive.exists()) {

        val backupArchive = new File(archivePath + ".backup")
        if (backupArchive.exists()) {
          backupArchive.delete()
        }

        //rename
        FileUtils.moveFile(archive, backupArchive)

        //open the existing archive for reading
        val zipFile = new ZipFile(backupArchive)
        val zop = new ZipOutputStream(new FileOutputStream(archivePath))

        //add EML
        addEML(zop, dataResourceUid)

        //add meta.xml - with multimedia extension
        addMeta(zop, List("Multimedia"))

        //add images CSV
        zop.putNextEntry(new ZipEntry("image.csv"))
        val imagesCSV = new FileInputStream(new File(workingDirSplitFiles + "/" + dataResourceUid))
        IOUtils.copy(imagesCSV, zop)
        zop.closeEntry()

        //add occurrences CSV from existing zip
        zop.putNextEntry(new ZipEntry("occurrence.csv"))
        val occurrenceInputStream: InputStream = {
          var stream: InputStream = null
          val entries = zipFile.entries
          while (entries.hasMoreElements) {
            val entry = entries.nextElement
            if (entry.getName == "occurrence.csv") {
              stream = zipFile.getInputStream(entry)
            }
          }
          stream
        }

        if (occurrenceInputStream != null) {
          IOUtils.copy(occurrenceInputStream, zop)
          occurrenceInputStream.close()
        }

        zop.closeEntry()
        zop.flush()
        zop.close()
      }
    }

    logger.info("Finished adding to existing archives.")
  }

  /**
    * Extract a Gzip
    *
    * @param inputFilePath
    * @param outputFilePath
    */
  def extractGzip(inputFilePath: String, outputFilePath: String): Unit = {
    val buffer = new Array[Byte](1024)
    try {
      val gzis = new GZIPInputStream(new FileInputStream(inputFilePath))
      val out = new FileOutputStream(outputFilePath)
      var len = gzis.read(buffer)
      while (len > 0) {
        out.write(buffer, 0, len)
        len = gzis.read(buffer)
      }
      gzis.close()
      out.close()
      logger.debug("GZIP extracted to: " + outputFilePath)
    } catch {
      case ex: IOException =>
        ex.printStackTrace()
    }
  }

  /**
    * Function to download a file
    *
    * @param urlStr
    * @param outputFile
    * @return
    */
  private def downloadToFile(urlStr: String, outputFile: String): Option[File] = try {
    val tmpFile = new File(outputFile)
    val url = new java.net.URL(urlStr)
    val in = url.openStream
    try {
      val out = new FileOutputStream(tmpFile)
      try {
        val buffer: Array[Byte] = new Array[Byte](1024)
        var numRead = 0
        while ( {
          numRead = in.read(buffer)
          numRead != -1
        }) {
          out.write(buffer, 0, numRead)
          out.flush
        }
      } finally {
        out.close()
      }
    } finally {
      in.close()
    }
    if (tmpFile.getTotalSpace > 0) {
      logger.debug("Temp file created: " + tmpFile.getAbsolutePath + ", file size: " + tmpFile.getTotalSpace)
      Some(tmpFile)
    } else {
      logger.debug(s"Failure to download image from  $urlStr")
      None
    }
  } catch {
    case e: Exception => {
      logger.error("Problem downloading media. URL:" + urlStr)
      logger.debug(e.getMessage, e)
      None
    }
  }
}