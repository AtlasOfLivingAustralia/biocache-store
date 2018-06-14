package au.org.ala.biocache.export

import java.util.zip.ZipOutputStream
import java.io.{FileOutputStream, OutputStreamWriter}
import java.util.zip.ZipEntry
import au.com.bytecode.opencsv.CSVWriter
import scala.io.Source
import org.apache.commons.io.FileUtils
import scala.util.parsing.json.JSON
import org.slf4j.LoggerFactory
import au.org.ala.biocache.Config
import au.org.ala.biocache.util.OptionParser
import util.matching.Regex
import au.org.ala.biocache.cmd.Tool

/**
 * Companion object for the DwCACreator class.
 */
object DwCACreator extends Tool {

  def cmd = "create-dwc"

  def desc = "Create Darwin Core Archive for a data resource"

  val logger = LoggerFactory.getLogger("DwCACreator")

  val defaultFields = List(
    "rowkey",
    "dataResourceUid",
    "catalogNumber",
    "collectionCode",
    "institutionCode",
    "scientificName_p",
    "recordedBy",
    "taxonConceptID_p",
    "taxonRank_p",
    "kingdom_p",
    "phylum_p",
    "classs_p",
    "order_p",
    "family_p",
    "genus_p",
    "decimalLatitude_p",
    "decimalLongitude_p",
    "coordinateUncertaintyInMeters_p",
    "maximumElevationInMeters",
    "minimumElevationInMeters",
    "minimumDepthInMeters",
    "maximumDepthInMeters",
    "geodeticDatum_p",
    "country_p",
    "stateProvince_p",
    "locality",
    "occurrenceStatus_p",
    "year_p",
    "month_p",
    "day_p",
    "eventDate_p",
    "eventDateEnd_p",
    "basisOfRecord_p",
    "identifiedBy",
    "occurrenceRemarks",
    "locationRemarks",
    "recordNumber",
    "vernacularName_p",
    "individualCount",
    "eventID",
    "dataGeneralizations_p"
  )

  def main(args: Array[String]): Unit = {

    var resourceUid = ""
    var directory = ""

    val parser = new OptionParser(help) {
      arg("data-resource-uid", "The UID of the data resource to load or 'all' to generate for all",
        { v: String => resourceUid = v }
      )
      arg("directory-to-dump", "skip the download and use local file",
        { v:String => directory = v }
      )
    }
    if(parser.parse(args)){
      val dwcc = new DwCACreator
      try {
        val dataResource2OutputStreams = getDataResourceUids.map { uid => (uid, dwcc.createOutputForCSV(directory, uid) ) }.toMap
        Config.persistenceManager.pageOverSelect("occ", (key, map) => {
          synchronized {
            val dr = map.getOrElse("dataResourceUid", "")
            if (dr != "") {
              val (zop, csv) = dataResource2OutputStreams.get(dr).get.get
              synchronized {

                val eventDate = {
                  val eventDate = map.getOrElse("eventDate_p", "")
                  val eventDateEnd = map.getOrElse("eventDateEnd_p", "")
                  if(eventDateEnd != "" && eventDate != "" && eventDate != eventDateEnd){
                    eventDate + "/" + eventDateEnd
                  } else {
                    eventDate
                  }
                }
                csv.writeNext(Array(
                  cleanValue(map.getOrElse("rowkey", "")),
                  cleanValue(map.getOrElse("catalogNumber",  "")),
                  cleanValue(map.getOrElse("collectionCode", "")),
                  cleanValue(map.getOrElse("institutionCode", "")),
                  cleanValue(map.getOrElse("recordNumber", "")),
                  cleanValue(map.getOrElse("basisOfRecord_p", "")),
                  cleanValue(map.getOrElse("recordedBy", "")),
                  cleanValue(map.getOrElse("occurrenceStatus_p", "")),
                  cleanValue(map.getOrElse("individualCount", "")),
                  cleanValue(map.getOrElse("scientificName_p", "")),
                  cleanValue(map.getOrElse("taxonConceptID_p", "")),
                  cleanValue(map.getOrElse("taxonRank_p", "")),
                  cleanValue(map.getOrElse("kingdom_p", "")),
                  cleanValue(map.getOrElse("phylum_p", "")),
                  cleanValue(map.getOrElse("classs_p", "")),
                  cleanValue(map.getOrElse("order_p", "")),
                  cleanValue(map.getOrElse("family_p", "")),
                  cleanValue(map.getOrElse("genus_p", "")),
                  cleanValue(map.getOrElse("vernacularName_p", "")),
                  cleanValue(map.getOrElse("decimalLatitude_p", "")),
                  cleanValue(map.getOrElse("decimalLongitude_p", "")),
                  cleanValue(map.getOrElse("geodeticDatum_p", "")),
                  cleanValue(map.getOrElse("coordinateUncertaintyInMeters_p", "")),
                  cleanValue(map.getOrElse("maximumElevationInMeters", "")),
                  cleanValue(map.getOrElse("minimumElevationInmeters", "")),
                  cleanValue(map.getOrElse("minimumDepthInMeters", "")),
                  cleanValue(map.getOrElse("maximumDepthInMeters", "")),
                  cleanValue(map.getOrElse("country_p", "")),
                  cleanValue(map.getOrElse("stateProvince_p", "")),
                  cleanValue(map.getOrElse("locality", "")),
                  cleanValue(map.getOrElse("locationRemarks", "")),
                  cleanValue(map.getOrElse("year_p", "")),
                  cleanValue(map.getOrElse("month_p", "")),
                  cleanValue(map.getOrElse("day_p", "")),
                  cleanValue(eventDate),
                  cleanValue(map.getOrElse("eventID", "")),
                  cleanValue(map.getOrElse("identifiedBy", "")),
                  cleanValue(map.getOrElse("occurrenceRemarks", "")),
                  cleanValue(map.getOrElse("dataGeneralizations_p", ""))
                ))
                csv.flush()
              }
            }
          }
          true
        }, 4, 1000, defaultFields:_*)

        dataResource2OutputStreams.values.foreach { zopAndCsv =>
          zopAndCsv.get._1.flush()
          zopAndCsv.get._1.closeEntry()
          zopAndCsv.get._1.close()
        }
      } catch {
        case e:Exception => logger.error(e.getMessage(), e)
      }
    }
  }

  def cleanValue(input:String) = if(input == null) "" else input.replaceAll("[\\t\\n\\r]", " ").trim

  // pattern to extract a data resource uid from a filter query , because the label show i18n value
  val dataResourcePattern = "(?:[\"]*)?(?:[a-z_]*_uid:\")([a-z0-9]*)(?:[\"]*)?".r

  def getDataResourceUids : Seq[String] = {
    val url = Config.biocacheServiceUrl + "/occurrences/search?q=*:*&facets=data_resource_uid&pageSize=0&flimit=-1"
    val jsonString = Source.fromURL(url).getLines.mkString
    val json = JSON.parseFull(jsonString).get.asInstanceOf[Map[String, String]]
    val results = json.get("facetResults").get.asInstanceOf[List[Map[String, String]]].head.get("fieldResult").get.asInstanceOf[List[Map[String, String]]]
    results.map(facet => {
      val fq = facet.get("fq").get
      parseFq(fq)
    }).filterNot(_.equals("Unknown"))
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
class DwCACreator {

  val logger = LoggerFactory.getLogger("DwCACreator")


  def createOutputForCSV(directory:String, dataResource:String) : Option[(ZipOutputStream, CSVWriter)] = {

    logger.info("Creating archive for " + dataResource)
    val zipFile = new java.io.File (
      directory +
      System.getProperty("file.separator") +
      dataResource +
      System.getProperty("file.separator") +
      dataResource +
      ".zip"
    )

    FileUtils.forceMkdir(zipFile.getParentFile)
    val zop = new ZipOutputStream(new FileOutputStream(zipFile))
    if(addEML(zop, dataResource)){
      addMeta(zop)
      zop.putNextEntry(new ZipEntry("occurrence.csv"))
      val writer = new CSVWriter(new OutputStreamWriter(zop))
      Some((zop, writer))
    } else {
      //no EML implies that a DWCA should not be generated.
      zop.close()
      FileUtils.deleteQuietly(zipFile)
      None
    }
  }

  def addEML(zop:ZipOutputStream, dr:String):Boolean ={
    try {
      zop.putNextEntry(new ZipEntry("eml.xml"))
      val content = Source.fromURL(Config.registryUrl + "/eml/" + dr).mkString
      zop.write(content.getBytes)
      zop.flush
      zop.closeEntry
      true
    } catch {
      case e:Exception => e.printStackTrace();false
    }
  }

  def addMeta(zop:ZipOutputStream) ={
    zop.putNextEntry(new ZipEntry("meta.xml"))
    val metaXml = <archive xmlns="http://rs.tdwg.org/dwc/text/" metadata="eml.xml">
      <core encoding="UTF-8" linesTerminatedBy="\r\n" fieldsTerminatedBy="," fieldsEnclosedBy="&quot;" ignoreHeaderLines="0" rowType="http://rs.tdwg.org/dwc/terms/Occurrence">
      <files>
            <location>occurrence.csv</location>
      </files>
            <id index="0"/>
            <field index="0"  term="http://rs.tdwg.org/dwc/terms/occurrenceID" />
            <field index="1"  term="http://rs.tdwg.org/dwc/terms/catalogNumber" />
            <field index="2"  term="http://rs.tdwg.org/dwc/terms/collectionCode" />
            <field index="3"  term="http://rs.tdwg.org/dwc/terms/institutionCode" />
            <field index="4"  term="http://rs.tdwg.org/dwc/terms/recordNumber" />
            <field index="5"  term="http://rs.tdwg.org/dwc/terms/basisOfRecord" default="HumanObservation" />
            <field index="6"  term="http://rs.tdwg.org/dwc/terms/recordedBy" />
            <field index="7"  term="http://rs.tdwg.org/dwc/terms/occurrenceStatus" />
            <field index="8"  term="http://rs.tdwg.org/dwc/terms/individualCount" />
            <field index="9"  term="http://rs.tdwg.org/dwc/terms/scientificName" />
            <field index="10" term="http://rs.tdwg.org/dwc/terms/taxonConceptID" />
            <field index="11" term="http://rs.tdwg.org/dwc/terms/taxonRank" />
            <field index="12" term="http://rs.tdwg.org/dwc/terms/kingdom" />
            <field index="13" term="http://rs.tdwg.org/dwc/terms/phylum" />
            <field index="14" term="http://rs.tdwg.org/dwc/terms/class" />
            <field index="15" term="http://rs.tdwg.org/dwc/terms/order" />
            <field index="16" term="http://rs.tdwg.org/dwc/terms/family" />
            <field index="17" term="http://rs.tdwg.org/dwc/terms/genus" />
            <field index="18" term="http://rs.tdwg.org/dwc/terms/vernacularName" />
            <field index="19" term="http://rs.tdwg.org/dwc/terms/decimalLatitude" />
            <field index="20" term="http://rs.tdwg.org/dwc/terms/decimalLongitude" />
            <field index="21" term="http://rs.tdwg.org/dwc/terms/geodeticDatum" />
            <field index="22" term="http://rs.tdwg.org/dwc/terms/coordinateUncertaintyInMeters" />
            <field index="23" term="http://rs.tdwg.org/dwc/terms/maximumElevationInMeters" />
            <field index="24" term="http://rs.tdwg.org/dwc/terms/minimumElevationInMeters" />
            <field index="25" term="http://rs.tdwg.org/dwc/terms/minimumDepthInMeters" />
            <field index="26" term="http://rs.tdwg.org/dwc/terms/maximumDepthInMeters" />
            <field index="27" term="http://rs.tdwg.org/dwc/terms/country" />
            <field index="28" term="http://rs.tdwg.org/dwc/terms/stateProvince" />
            <field index="29" term="http://rs.tdwg.org/dwc/terms/locality" />
            <field index="30" term="http://rs.tdwg.org/dwc/terms/locationRemarks" />
            <field index="31" term="http://rs.tdwg.org/dwc/terms/year" />
            <field index="32" term="http://rs.tdwg.org/dwc/terms/month" />
            <field index="33" term="http://rs.tdwg.org/dwc/terms/day" />
            <field index="34" term="http://rs.tdwg.org/dwc/terms/eventDate" />
            <field index="35" term="http://rs.tdwg.org/dwc/terms/eventID" />
            <field index="36" term="http://rs.tdwg.org/dwc/terms/identifiedBy" />
            <field index="37" term="http://rs.tdwg.org/dwc/terms/occurrenceRemarks" />
            <field index="38" term="http://rs.tdwg.org/dwc/terms/dataGeneralizations" />
      </core>
    </archive>
    //add the XML
    zop.write("""<?xml version="1.0"?>""".getBytes)
    zop.write("\n".getBytes)
    zop.write(metaXml.mkString("\n").getBytes)
    zop.flush
    zop.closeEntry
  }
}