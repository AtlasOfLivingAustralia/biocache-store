package au.org.ala.biocache.processor

import au.org.ala.biocache.model.{FullRecord, QualityAssertion}
import au.org.ala.biocache.parser.CollectorNameParser
import au.org.ala.biocache.vocab._
import org.apache.commons.lang.StringUtils

import scala.collection.mutable.ArrayBuffer

/**
  * A processor of miscellaneous information.
  */
class MiscellaneousProcessor extends Processor {

  val LIST_DELIM = ";".r
  val interactionPattern = """([A-Za-z]*):([\x00-\x7F\s]*)""".r

  import AssertionCodes._
  import AssertionStatus._

  def process(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {
    val assertions = new ArrayBuffer[QualityAssertion]
    processImages(guid, raw, processed, assertions)
    processInteractions(guid, raw, processed)
    processEstablishmentMeans(raw, processed, assertions)
    processIdentification(raw, processed, assertions)
    processCollectors(raw, processed, assertions)
    processMiscOccurrence(raw, processed, assertions)
    processOccurrenceStatus(raw, processed, assertions)
    assertions.toArray
  }

  /**
    * Process the occurrence status values.
    *
    * @param raw
    * @param processed
    * @param assertions
    */
  def processOccurrenceStatus(raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]) {
    val (processedValue, qaOption) = processOccurrenceStatus(raw.occurrence.occurrenceStatus)
    processed.occurrence.occurrenceStatus = processedValue
    if (!qaOption.isEmpty) {
      assertions += qaOption.get
    }
  }

  def processOccurrenceStatus(rawOccurrenceStatus: String): (String, Option[QualityAssertion]) = {
    if (StringUtils.isNotBlank(rawOccurrenceStatus)) {
      val matchedTerm = OccurrenceStatus.matchTerm(rawOccurrenceStatus)
      if (matchedTerm.isEmpty) {
        ("unknown", Some(QualityAssertion(UNRECOGNISED_OCCURRENCE_STATUS)))
      } else {
        (matchedTerm.get.canonical, None)
      }
    } else {
      //assume present
      ("present", Some(QualityAssertion(ASSUMED_PRESENT_OCCURRENCE_STATUS)))
    }
  }

  def processMiscOccurrence(raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]) {
    if (StringUtils.isBlank(raw.occurrence.catalogNumber)) {
      assertions += QualityAssertion(MISSING_CATALOGUENUMBER, "No catalogue number provided")
    } else {
      assertions += QualityAssertion(MISSING_CATALOGUENUMBER, PASSED)
    }
    //check to see if the source data has been provided in a generalised form
    if (StringUtils.isNotBlank(raw.occurrence.dataGeneralizations)) {
      assertions += QualityAssertion(DATA_ARE_GENERALISED)
    } else {
      //data not generalised by the provider
      assertions += QualityAssertion(DATA_ARE_GENERALISED, PASSED)
    }
  }

  /**
    * parse the collector string to place in a consistent format
    */
  def processCollectors(raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]) = {
    if (StringUtils.isNotBlank(raw.occurrence.recordedBy)) {
      val parsedCollectors = CollectorNameParser.parseForList(raw.occurrence.recordedBy)
      if (parsedCollectors.isDefined) {
        processed.occurrence.recordedBy = parsedCollectors.get.filter(_ != null).mkString("|")
        assertions += QualityAssertion(RECORDED_BY_UNPARSABLE, 1)
      } else {
        //println("Unable to parse: " + raw.occurrence.recordedBy)
        assertions += QualityAssertion(RECORDED_BY_UNPARSABLE, "Can not parse recordedBy")
      }
    }
  }

  def processEstablishmentMeans(raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]): Unit = {
    //2012-0202: At this time AVH is the only data resource to support this. In the future it may be necessary for the value to be a list...
    //handle the "cultivated" type
    //2012-07-13: AVH has moved this to establishmentMeans and has also include nativeness
    if (StringUtils.isNotBlank(raw.occurrence.establishmentMeans)) {
      val ameans = LIST_DELIM.split(raw.occurrence.establishmentMeans)
      val newmeans = ameans.map(means => {
        val term = EstablishmentMeans.matchTerm(means)
        if (term.isDefined) term.get.getCanonical else ""
      }).filter(_.length > 0)

      if (!newmeans.isEmpty) {
        processed.occurrence.establishmentMeans = newmeans.mkString("; ")
      }

      //check to see if the establishment mean corresponds to culitvated or escaped
      //FIXME extract to a vocabulary
      val cultEscaped = newmeans.find(em => em == "cultivated" || em == "assumed to be cultivated" || em == "formerly cultivated (extinct)" || em == "possibly cultivated" || em == "presumably cultivated")
      if (cultEscaped.isDefined) {
        assertions += QualityAssertion(OCCURRENCE_IS_CULTIVATED_OR_ESCAPEE)
      } else {
        //represents a natural occurrence. not cultivated ot escaped
        assertions += QualityAssertion(OCCURRENCE_IS_CULTIVATED_OR_ESCAPEE, 1)
      }
    }
  }

  def processIdentification(raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]) = {
    //check missing identification qualifier
    if (raw.identification.identificationQualifier == null)
      assertions += QualityAssertion(MISSING_IDENTIFICATIONQUALIFIER, "Missing identificationQualifier")
    else
      assertions += QualityAssertion(MISSING_IDENTIFICATIONQUALIFIER, PASSED)
    //check missing identifiedBy
    if (raw.identification.identifiedBy == null)
      assertions += QualityAssertion(MISSING_IDENTIFIEDBY, "Missing identifiedBy")
    else
      assertions += QualityAssertion(MISSING_IDENTIFIEDBY, PASSED)
    //check missing identification references
    if (raw.identification.identificationReferences == null)
      assertions += QualityAssertion(MISSING_IDENTIFICATIONREFERENCES, "Missing identificationReferences")
    else
      assertions += QualityAssertion(MISSING_IDENTIFICATIONREFERENCES, PASSED)
    //check missing date identified
    if (raw.identification.dateIdentified == null)
      assertions += QualityAssertion(MISSING_DATEIDENTIFIED, "Missing dateIdentified")
    else
      assertions += QualityAssertion(MISSING_DATEIDENTIFIED, 1)
  }

  /**
    * more sophisticated parsing of the string. ATM we are only supporting the structure for dr642
    * TODO support multiple interactions
    *
    * @param guid
    * @param raw
    * @param processed
    */
  def processInteractions(guid: String, raw: FullRecord, processed: FullRecord) = {
    //interactions are supplied as part of the associatedTaxa string
    //TODO more sophisticated parsing of the string. ATM we are only supporting the structure for dr642
    //TODO support multiple interactions
    if (raw.occurrence.associatedTaxa != null && !raw.occurrence.associatedTaxa.isEmpty) {
      val interaction = parseInteraction(raw.occurrence.associatedTaxa)
      if (!interaction.isEmpty) {
        val term = Interactions.matchTerm(interaction.get)
        if (!term.isEmpty) {
          processed.occurrence.interactions = Array(term.get.getCanonical)
        }
      }
    }
  }

  def parseInteraction(raw: String): Option[String] = raw match {
    case interactionPattern(interaction, taxa) => Some(interaction)
    case _ => None
  }

  /**
    * validates that the associated media is a valid image url
    */
  def processImages(guid: String, raw: FullRecord, processed: FullRecord, assertions: ArrayBuffer[QualityAssertion]) = {
    processed.occurrence.images = raw.occurrence.images
    processed.occurrence.sounds = raw.occurrence.sounds
    processed.occurrence.videos = raw.occurrence.videos
  }

  def skip(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {
    var assertions = new ArrayBuffer[QualityAssertion]

    //get the data resource information to check if it has mapped collections
    if (lastProcessed.isDefined) {
      //MiscellaneousProcessor has low overhead, do not skip
      process(guid, raw, processed, lastProcessed)
    }

    assertions.toArray
  }

  def getName = "image"
}
