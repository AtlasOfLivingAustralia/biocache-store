package au.org.ala.biocache.processor

import au.org.ala.biocache.Config
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions
import scala.collection.mutable.{ArrayBuffer, HashMap}
import au.org.ala.names.model.LinnaeanRankClassification
import org.apache.commons.lang.StringUtils
import au.org.ala.biocache.caches.{CommonNameDAO, TaxonProfileDAO, ClassificationDAO, AttributionDAO}
import au.org.ala.biocache.util.BiocacheConversions
import au.org.ala.biocache.model.{Attribution, QualityAssertion, FullRecord, Classification}
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.vocab.{AssertionStatus, Kingdoms, DwC, AssertionCodes}

/**
 * A processor of taxonomic information.
 */
class ClassificationProcessor extends Processor {

  val logger = LoggerFactory.getLogger("ClassificationProcessor")

  val nationalChecklistIdentifierPattern = Config.nationalChecklistIdentifierPattern.r

  /** Pattern to match names with question marks (indicating uncertainty of identification */
  val questionPattern = """([\x00-\x7F\s]*)\?([\x00-\x7F\s]*)""".r
  val affPattern = """([\x00-\x7F\s]*) aff[#!?\\.]?([\x00-\x7F\s]*)""".r
  val cfPattern = """([\x00-\x7F\s]*) cf[#!?\\.]?([\x00-\x7F\s]*)""".r

  import AssertionCodes._
  import AssertionStatus._
  import BiocacheConversions._

  import JavaConversions._

  /**
   * Parse the hints into a usable map with rank -> Set.
   */
  def parseHints(taxonHints: List[String]): Map[String, Set[String]] = {
    //parse taxon hints into rank : List of
    val rankSciNames = new HashMap[String, Set[String]]
    val pairs = taxonHints.map(x => x.split(":"))
    pairs.foreach { pair =>
      val values = rankSciNames.getOrElse(pair(0), Set())
      rankSciNames.put(pair(0), values + pair(1).trim.toLowerCase)
    }
    rankSciNames.toMap
  }

  /**
   * Returns false if the any of the taxonomic hints conflict with the classification
   */
  def isMatchValid(cl: LinnaeanRankClassification, hintMap: Map[String, Set[String]]): (Boolean, String) = {
    //are there any conflicts??
    hintMap.keys.foreach { rank =>
      val (conflict, comment) = {
        rank match {
          case "kingdom" => (hasConflict(rank, cl.getKingdom, hintMap), "Kingdom:" + cl.getKingdom)
          case "phylum" => (hasConflict(rank, cl.getPhylum, hintMap), "Phylum:" + cl.getPhylum)
          case "class" => (hasConflict(rank, cl.getKlass, hintMap), "Class:" + cl.getKlass)
          case "order" => (hasConflict(rank, cl.getOrder, hintMap), "Order:" + cl.getOrder)
          case "family" => (hasConflict(rank, cl.getFamily, hintMap), "Family:" + cl.getFamily)
          case _ => (false, "")
        }
      }
      if (conflict) {
        return (false, comment)
      }
    }
    (true, "")
  }

  private def hasConflict(rank: String, taxon: String, hintMap: Map[String, Set[String]]): Boolean = {
    taxon != null && !hintMap.get(rank).get.contains(taxon.toLowerCase)
  }

  private def hasMatchToDefault(rank: String, taxon: String, classification: Classification): Boolean = {
    def term = DwC.matchTerm(rank)
    def field = if (term.isDefined) term.get.canonical else rank
    taxon != null && taxon.equalsIgnoreCase(classification.getProperty(field).getOrElse(""))
  }

  /**
   * Add the details of the match type to the record.
   *
   * @param nameMetrics
   * @param processed
   * @param assertions
   */
  private def setMatchStats(nameMetrics:au.org.ala.names.model.MetricsResultDTO, processed:FullRecord, assertions:ArrayBuffer[QualityAssertion]){
    //set the parse type and errors for all results before continuing
    processed.classification.nameParseType = if (nameMetrics.getNameType != null) {
      nameMetrics.getNameType.toString
    } else {
      null
    }
    //add the taxonomic issues for the match
    processed.classification.taxonomicIssue = if (nameMetrics.getErrors != null) {
      nameMetrics.getErrors.toList.map(_.toString).toArray
    } else {
      Array("noIssue")
    }
    //check the name parse tye to see if the scientific name was valid
    if (processed.classification.nameParseType == "blacklisted") {
      assertions += QualityAssertion(INVALID_SCIENTIFIC_NAME)
    } else {
      assertions += QualityAssertion(INVALID_SCIENTIFIC_NAME, PASSED)
    }
  }

  /**
   * Perform a set of data quality tests associated with taxonomy.
   *
   * @param raw
   * @param processed
   * @param assertions
   */
  private def doQualityTests(raw:FullRecord, processed:FullRecord, assertions:ArrayBuffer[QualityAssertion]){

    //test for the missing taxon rank
    if (StringUtils.isBlank(raw.classification.taxonRank)) {
      assertions += QualityAssertion(MISSING_TAXONRANK, "Missing taxonRank")
    } else {
      assertions += QualityAssertion(MISSING_TAXONRANK, PASSED)
    }

    //test that a scientific name or vernacular name has been supplied
    if (StringUtils.isBlank(raw.classification.scientificName) && StringUtils.isBlank(raw.classification.vernacularName)) {
      assertions += QualityAssertion(NAME_NOT_SUPPLIED, "No scientificName or vernacularName has been supplied. Name match will be based on a constructed name.")
    } else {
      assertions += QualityAssertion(NAME_NOT_SUPPLIED, PASSED)
    }

    //test for mismatch in kingdom
    if (StringUtils.isNotBlank(raw.classification.kingdom)) {
      val matchedKingdom = Kingdoms.matchTerm(raw.classification.kingdom)
      if (matchedKingdom.isDefined) {
        //the supplied kingdom is recognised
        assertions += QualityAssertion(UNKNOWN_KINGDOM, PASSED)
      } else {
        assertions += QualityAssertion(UNKNOWN_KINGDOM, "The supplied kingdom is not recognised")
      }
    }
  }

  /**
   * Match the supplied classification for this record to the backbone taxonomy of the system,
   * and perform a set of data quality tests.
   */
  def process(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {
    var assertions = new ArrayBuffer[QualityAssertion]

    //perform quality tests
    doQualityTests(raw, processed, assertions)

    try {
      //update the raw with the "default" values for this resource if necessary
      //this will help avoid homonym issues for records without a full classification
      if (processed.defaultValuesUsed) {
        if (raw.classification.kingdom == null && processed.classification.kingdom != null) {
          raw.classification.kingdom = processed.classification.kingdom
        }
        if (raw.classification.phylum == null && processed.classification.phylum != null) {
          raw.classification.phylum = processed.classification.phylum
        }
        if (raw.classification.classs == null && processed.classification.classs != null) {
          raw.classification.classs = processed.classification.classs
        }
        if (raw.classification.order == null && processed.classification.order != null) {
          raw.classification.order = processed.classification.order
        }
        if (raw.classification.family == null && processed.classification.family != null) {
          raw.classification.family = processed.classification.family
        }
      }

      //do a name match
      val nameMetrics = ClassificationDAO.get(raw.classification).getOrElse(null)
      if (nameMetrics != null) {

        val nsr = nameMetrics.getResult

        //store the matched classification
        if (nsr != null) {
          //The name is recognised:
          assertions += QualityAssertion(NAME_NOTRECOGNISED, PASSED)
          val classification = nsr.getRankClassification

          //Check to see if the classification fits in with the supplied taxonomic hints
          //get the taxonomic hints from the collection or data resource
          var attribution: Option[Attribution] = AttributionDAO.getDataResourceByUid(raw.attribution.dataResourceUid)
          if (attribution.isDefined && attribution.get.hasMappedCollections) {
            attribution = AttributionDAO.getByCodes(raw.occurrence.institutionCode, raw.occurrence.collectionCode)
          }

          var hintsPassed = true
          if (!attribution.isEmpty) {
            logger.debug("Checking taxonomic hints")
            val taxonHints = attribution.get.taxonomicHints
            if (taxonHints != null && !taxonHints.isEmpty) {
              val (isValid, comment) = isMatchValid(classification, attribution.get.retrieveParseHints)
              if (!isValid) {
                if (logger.isInfoEnabled){
                  val taxonHintDebug = taxonHints.mkString(",")
                  val dataResourceUid = raw.attribution.dataResourceUid
                  logger.info(s"Conflict in matched classification. [$dataResourceUid] GUID: $guid, Matched: $comment, Taxonomic hints in use: $taxonHintDebug")
                }
                hintsPassed = false
                processed.classification.nameMatchMetric = "matchFailedHint"
                assertions += QualityAssertion(RESOURCE_TAXONOMIC_SCOPE_MISMATCH, comment)
              } else if (!attribution.get.retrieveParseHints.isEmpty) {
                //the taxonomic hints passed
                assertions += QualityAssertion(RESOURCE_TAXONOMIC_SCOPE_MISMATCH, PASSED)
              }
            }
          }

          //check for default match before updating the classification.
          val hasDefaultMatch = {
            processed.defaultValuesUsed &&
              nsr.getRank() != null &&
              hasMatchToDefault(nsr.getRank().getRank(), nsr.getRankClassification().getScientificName(), processed.classification)
          }

          //store ".p" values if the taxonomic hints passed
          if (hintsPassed) {
            processed.classification = nsr
          }

          //check to see if the classification has been matched to a default value
          if (hasDefaultMatch) {
            //indicates that a default value was used to make the higher level match
            processed.classification.nameMatchMetric = "defaultHigherMatch"
          }

          //add a common name
          processed.classification.vernacularName = CommonNameDAO.getByGuid(nsr.getLsid).getOrElse(null)

          //try to apply the vernacular name, species habitats
          val taxonProfile = TaxonProfileDAO.getByGuid(nsr.getLsid)
          if (!taxonProfile.isEmpty) {
            if (taxonProfile.get.habitats != null) {
              processed.classification.speciesHabitats = taxonProfile.get.habitats
            }
            //second attempt to add common name
            if (taxonProfile.get.commonName != null && processed.classification.vernacularName == null) {
              processed.classification.vernacularName = taxonProfile.get.commonName
            }
          }

          //add the taxonomic rating for the raw name
          val scientificName = if (raw.classification.scientificName != null) {
            raw.classification.scientificName
          } else if (raw.classification.species != null) {
            raw.classification.species
          } else if (raw.classification.specificEpithet != null && raw.classification.genus != null) {
            raw.classification.genus + " " + raw.classification.specificEpithet
          } else {
            null
          }

          setMatchStats(nameMetrics, processed, assertions)

          //is the name in the NSLs ???
          if (Config.nationalChecklistIdentifierPattern != "" && nationalChecklistIdentifierPattern.findFirstMatchIn(nsr.getLsid).isEmpty) {
            assertions += QualityAssertion(NAME_NOT_IN_NATIONAL_CHECKLISTS, "Record not attached to concept in national species lists")
          } else {
            assertions += QualityAssertion(NAME_NOT_IN_NATIONAL_CHECKLISTS, PASSED)
          }

        } else if (nameMetrics.getErrors.contains(au.org.ala.names.model.ErrorType.HOMONYM)) {
          if(logger.isDebugEnabled){
            logger.debug("[QualityAssertion] A homonym was detected (with  no higher level match), classification for Kingdom: " +
              raw.classification.kingdom + ", Family:" + raw.classification.family + ", Genus:" + raw.classification.genus +
              ", Species: " + raw.classification.species + ", Epithet: " + raw.classification.specificEpithet)
          }
          processed.classification.nameMatchMetric = "noMatch"
          setMatchStats(nameMetrics, processed, assertions)
          assertions += QualityAssertion(HOMONYM_ISSUE, "A homonym was detected in supplied classification.")
        } else {
          if(logger.isDebugEnabled) {
            logger.debug("[QualityAssertion] No match for record, classification for Kingdom: " +
              raw.classification.kingdom + ", Family:" + raw.classification.family + ", Genus:" + raw.classification.genus +
              ", Species: " + raw.classification.species + ", Epithet: " + raw.classification.specificEpithet)
          }
          processed.classification.nameMatchMetric = "noMatch"
          setMatchStats(nameMetrics, processed, assertions)
          assertions += QualityAssertion(NAME_NOTRECOGNISED, "Name not recognised")
        }
      } else {
        if(logger.isDebugEnabled) {
          logger.debug("[QualityAssertion] No match for record, classification for Kingdom: " +
            raw.classification.kingdom + ", Family:" + raw.classification.family + ", Genus:" + raw.classification.genus +
            ", Species: " + raw.classification.species + ", Epithet: " + raw.classification.specificEpithet)
        }
        processed.classification.nameMatchMetric = "noMatch"
        assertions += QualityAssertion(NAME_NOTRECOGNISED, "Name not recognised")
      }
    } catch {
      case e: Exception => logger.error("Exception during classification match for record " + guid, e)
    }
    assertions.toArray
  }

  def skip(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {
    var assertions = new ArrayBuffer[QualityAssertion]

    if (lastProcessed.isDefined) {
      assertions ++= lastProcessed.get.findAssertions(Array(HOMONYM_ISSUE.code, NAME_NOTRECOGNISED.code,
        NAME_NOT_IN_NATIONAL_CHECKLISTS.code, INVALID_SCIENTIFIC_NAME.code, MISSING_TAXONRANK.code,
        NAME_NOT_SUPPLIED.code, UNKNOWN_KINGDOM.code, RESOURCE_TAXONOMIC_SCOPE_MISMATCH.code))

      //update the details from lastProcessed
      processed.classification = lastProcessed.get.classification
    }

    assertions.toArray
  }

  def getName = FullRecordMapper.taxonomicalQa
}
