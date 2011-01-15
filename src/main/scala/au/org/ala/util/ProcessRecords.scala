package au.org.ala.util

import au.org.ala.biocache.HabitatMap
import au.org.ala.biocache.TaxonProfileDAO
import au.org.ala.biocache.AttributionDAO
import org.apache.commons.lang.time.DateUtils
import java.util.Calendar
import org.apache.commons.lang.time.DateFormatUtils
import org.wyki.cassandra.pelops.Pelops
import au.org.ala.biocache.DAO
import au.org.ala.biocache.TypeStatus
import au.org.ala.biocache.BasisOfRecord
import au.org.ala.biocache.AssertionCodes
import au.org.ala.biocache.QualityAssertion
import au.org.ala.biocache.States
import au.org.ala.biocache.Event
import au.org.ala.biocache.Classification
import au.org.ala.biocache.Location
import au.org.ala.biocache.LocationDAO
import au.org.ala.biocache.Version
import au.org.ala.checklist.lucene.HomonymException
import au.org.ala.data.util.RankType
import au.org.ala.biocache.Occurrence
import au.org.ala.data.model.LinnaeanRankClassification
import au.org.ala.checklist.lucene.CBIndexSearch
import au.org.ala.biocache.OccurrenceDAO
/**
 * 1. Classification matching
 * 	- include a flag to indicate record hasnt been matched to NSLs
 * 
 * 2. Parse locality information
 * 	- "Vic" -> Victoria
 * 
 * 3. Point matching
 * 	- parse latitude/longitude
 * 	- retrieve associated point mapping
 * 	- check state supplied to state point lies in
 * 	- marine/non-marine/limnetic (need a webservice from BIE)
 * 
 * 4. Type status normalization
 * 	- use GBIF's vocabulary
 * 
 * 5. Date parsing
 * 	- date validation
 * 	- support for date ranges
 * 
 * 6. Collectory lookups for attribution chain
 */
object ProcessRecords {

  def main(args: Array[String]): Unit = {

    var counter = 0
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis

    //page over all records and process
    OccurrenceDAO.pageOverAll(Version.Raw, o => {
      counter += 1
      if (!o.isEmpty) {

        val rawOccurrence = o.get._1
        val rawClassification = o.get._2
        val rawLocation = o.get._3
        val rawEvent = o.get._4

        var processedOccurrence = rawOccurrence.clone
        var processedClassification = new Classification
        var processedLocation = rawLocation.clone
        var processedEvent = rawEvent.clone

        if (counter % 1000 == 0) {
          finishTime = System.currentTimeMillis
          println(counter + " >> Last key : " + rawOccurrence.uuid + ", records per sec: " + 1000f / (((finishTime - startTime).toFloat) / 1000f))
          startTime = System.currentTimeMillis
        }

        //find a classification in NSLs
        processClassification(rawOccurrence, rawClassification, processedClassification)

        //perform gazetteer lookups - just using point hash for now
        processLocation(rawOccurrence, rawLocation, processedLocation, processedClassification)

        //temporal processing
        processEvent(rawOccurrence, rawEvent, processedEvent)

        //basis of record parsing
        processBasisOfRecord(rawOccurrence, processedOccurrence)

        //type status normalisation
        processTypeStatus(rawOccurrence, processedOccurrence)

        //process the attribution - call out to the Collectory...
        processAttribution(rawOccurrence, processedOccurrence)

        //perform SDS lookups - retrieve from BIE for now....

        //store the occurrence
        OccurrenceDAO.updateOccurrence(rawOccurrence.uuid, processedOccurrence, Version.Processed)
        OccurrenceDAO.updateOccurrence(rawOccurrence.uuid, processedLocation, Version.Processed)
        OccurrenceDAO.updateOccurrence(rawOccurrence.uuid, processedClassification, Version.Processed)
        OccurrenceDAO.updateOccurrence(rawOccurrence.uuid, processedEvent, Version.Processed)
      }
    })
    Pelops.shutdown
  }

  /**
   * select icm.institution_uid, icm.collection_uid,  ic.code, ic.name, ic.lsid, cc.code from inst_coll_mapping icm
   * inner join institution_code ic ON ic.id = icm.institution_code_id
   * inner join collection_code cc ON cc.id = icm.collection_code_id
   * limit 10;
   */
  def processAttribution(rawOccurrence: Occurrence, processedOccurrence: Occurrence) {
    val attribution = AttributionDAO.getAttibutionByCodes(rawOccurrence.institutionCode, rawOccurrence.collectionCode)
    if (!attribution.isEmpty) {
      OccurrenceDAO.updateOccurrence(rawOccurrence.uuid, attribution.get, Version.Processed)
    }
  }

  /**
   * Date parsing - this is pretty much copied from GBIF source code and needs
   * splitting into several methods
   */
  def processEvent(rawOccurrence: Occurrence, rawEvent: Event, processedEvent: Event) {

    var year = -1
    var month = -1
    var day = -1
    var date: Option[java.util.Date] = None

    var invalidDate = false;
    val now = new java.util.Date
    val currentYear = DateFormatUtils.format(now, "yyyy").toInt

    try {
      if (rawEvent.year != null) {
        year = rawEvent.year.toInt
        if (year < 0 || year > currentYear) {
          invalidDate = true
          year = -1
        }
      }
    } catch {
      case e: NumberFormatException => {
        invalidDate = true
        year = -1
      }
    }

    try {
      if (rawEvent.month != null)
        month = rawEvent.month.toInt
      if (month < 1 || month > 12) {
        month = -1
        invalidDate = true
      }
    } catch {
      case e: NumberFormatException => {
        invalidDate = true
        month = -1
      }
    }

    try {
      if (rawEvent.day != null)
        day = rawEvent.day.toInt
      if (day < 0 || day > 31) {
        day = -1
        invalidDate = true
      }
    } catch {
      case e: NumberFormatException => {
        invalidDate = true
        day = -1
      }
    }

    if (year > 0) {
      if (year < 100) {
        if (year > currentYear % 100) {
          // Must be in last century
          year += ((currentYear / 100) - 1) * 100;
        } else {
          // Must be in this century
          year += (currentYear / 100) * 100;
        }
      } else if (year >= 100 && year < 1700) {
        year = -1
        invalidDate = true;
      }
    }

    //construct
    if (year != -1 && month != -1 && day != -1) {
      try {
        val calendar = Calendar.getInstance
        calendar.set(year, month - 1, day, 12, 0, 0);
        date = Some(new java.util.Date(calendar.getTimeInMillis()))
      } catch {
        case e: Exception => {
          invalidDate = true
        }
      }
    }

    if (year != -1) processedEvent.year = year.toString
    if (month != -1) processedEvent.month = month.toString
    if (day != -1) processedEvent.day = day.toString
    if (!date.isEmpty) {
      processedEvent.eventDate = DateFormatUtils.format(date.get, "yyyy-MM-dd")
    }

    if (date.isEmpty && rawEvent.eventDate != null && !rawEvent.eventDate.isEmpty) {
      //TODO handle these formats
      //			"1963-03-08T14:07-0600" is 8 Mar 1963 2:07pm in the time zone six hours earlier than UTC, 
      //			"2009-02-20T08:40Z" is 20 Feb 2009 8:40am UTC, "1809-02-12" is 12 Feb 1809, 
      //			"1906-06" is Jun 1906, "1971" is just that year, 
      //			"2007-03-01T13:00:00Z/2008-05-11T15:30:00Z" is the interval between 1 Mar 2007 1pm UTC and 
      //			11 May 2008 3:30pm UTC, "2007-11-13/15" is the interval between 13 Nov 2007 and 15 Nov 2007
      try {
        val eventDateParsed = DateUtils.parseDate(rawEvent.eventDate,
          Array("yyyy-MM-dd", "yyyy-MM-ddThh:mm-ss", "2009-02-20T08:40Z"))
        processedEvent.eventDate = DateFormatUtils.format(date.get, "yyyy-MM-dd")
      } catch {
        case e: Exception => {
          //handle "1906-06"
          invalidDate = true
        }
      }
    }

    //deal with verbatim date
    if (date.isEmpty && rawEvent.verbatimEventDate != null && !rawEvent.verbatimEventDate.isEmpty) {
      try {
        val eventDate = rawEvent.verbatimEventDate.split("/").first
        val eventDateParsed = DateUtils.parseDate(eventDate,
          Array("yyyy-MM-dd", "yyyy-MM-ddThh:mm-ss", "2009-02-20T08:40Z"))
      } catch {
        case e: Exception => {
          invalidDate = true
        }
      }
    }

    if (invalidDate) {
      //			or.setOtherIssueBits(IndexingIssue.OTHER_INVALID_DATE.getBit());
      //			GbifLogMessage rangeMessage = gbifLogUtils.createGbifLogMessage(context, LogEvent.EXTRACT_GEOSPATIALISSUE,
      //			    "Invalid or unparsable date");
      //			rangeMessage.setCountOnly(true);
      //			logger.warn(rangeMessage);			
      var qa = new QualityAssertion
      qa.assertionCode = AssertionCodes.OTHER_INVALID_DATE.code
      qa.positive = false
      OccurrenceDAO.addQualityAssertion(rawOccurrence.uuid, qa, AssertionCodes.OTHER_INVALID_DATE)
    }
  }

  /**
   * Process the type status
   */
  def processTypeStatus(rawOccurrence: Occurrence, processedOccurrence: Occurrence) {

    if (rawOccurrence.typeStatus != null && rawOccurrence.typeStatus.isEmpty) {
      val term = TypeStatus.matchTerm(rawOccurrence.typeStatus)
      if (term.isEmpty) {
        //add a quality assertion
        val qa = new QualityAssertion
        qa.positive = false
        qa.assertionCode = AssertionCodes.OTHER_UNRECOGNISED_TYPESTATUS.code
        qa.comment = "Unrecognised type status"
        qa.userId = "system"
        OccurrenceDAO.addQualityAssertion(rawOccurrence.uuid, qa,  AssertionCodes.OTHER_UNRECOGNISED_TYPESTATUS)
      } else {
        processedOccurrence.basisOfRecord = term.get.canonical
      }
    }
  }

  /**
   * Process basis of record
   */
  def processBasisOfRecord(rawOccurrence: Occurrence, processedOccurrence: Occurrence) {

    if (rawOccurrence.basisOfRecord == null || rawOccurrence.basisOfRecord.isEmpty) {
      //add a quality assertion
      val qa = new QualityAssertion
      qa.positive = false
      qa.assertionCode = AssertionCodes.OTHER_MISSING_BASIS_OF_RECORD.code
      qa.comment = "Missing basis of record"
      qa.userId = "system"
      OccurrenceDAO.addQualityAssertion(rawOccurrence.uuid, qa,  AssertionCodes.OTHER_UNRECOGNISED_TYPESTATUS)
    } else {
      val term = BasisOfRecord.matchTerm(rawOccurrence.basisOfRecord)
      if (term.isEmpty) {
        //add a quality assertion
        println("[QualityAssertion] " + rawOccurrence.uuid + ", unrecognised BoR: " + rawOccurrence.uuid + ", BoR:" + rawOccurrence.basisOfRecord)
        val qa = new QualityAssertion
        qa.positive = false
        qa.assertionCode = AssertionCodes.OTHER_BADLY_FORMED_BASIS_OF_RECORD.code
        qa.comment = "Unrecognised basis of record"
        qa.userId = "system"
        OccurrenceDAO.addQualityAssertion(rawOccurrence.uuid, qa,  AssertionCodes.OTHER_UNRECOGNISED_TYPESTATUS)
      } else {
        processedOccurrence.basisOfRecord = term.get.canonical
      }
    }
  }

  
  /**
   * Process geospatial details
   */
  def processLocation(rawOccurrence:Occurrence, raw:Location, processed:Location, processedClassification:Classification) {
    //retrieve the point
    if (raw.decimalLatitude != null && raw.decimalLongitude != null) {

      //TODO validate decimal degrees
      processed.decimalLatitude = raw.decimalLatitude
      processed.decimalLongitude = raw.decimalLongitude

      //validate coordinate accuracy (coordinateUncertaintyInMeters) and coordinatePrecision (precision - A. Chapman)


      
      
      
      
      
      //generate coordinate accuracy if not supplied
      val point = LocationDAO.getLocationByLatLon(raw.decimalLatitude, raw.decimalLongitude);
      if (!point.isEmpty) {

        //add state information
        processed.stateProvince = point.get.stateProvince
        processed.ibra = point.get.ibra
        processed.imcra = point.get.imcra
        processed.lga = point.get.lga
        if(point.get.habitat!=null) println("uuid: "+rawOccurrence.uuid+". habitat: "+point.get.habitat)
        processed.habitat = point.get.habitat

        //check matched stateProvince
        if (processed.stateProvince != null && raw.stateProvince != null) {
          //quality assertions
          val stateTerm = States.matchTerm(raw.stateProvince)

          if (!stateTerm.isEmpty && !processed.stateProvince.equalsIgnoreCase(stateTerm.get.canonical)) {
            println("[QualityAssertion] " + rawOccurrence.uuid + ", processed:" + processed.stateProvince + ", raw:" + raw.stateProvince)
            //add a quality assertion
            val qa = new QualityAssertion
            qa.positive = false
            qa.assertionCode = AssertionCodes.GEOSPATIAL_STATE_COORDINATE_MISMATCH.code
            qa.comment = "Supplied: " + stateTerm.get.canonical + ", calculated: " + processed.stateProvince
            qa.userId = "system"
            //store the assertion
            OccurrenceDAO.addQualityAssertion(rawOccurrence.uuid, qa, AssertionCodes.GEOSPATIAL_STATE_COORDINATE_MISMATCH)
          }
        }

        //check marine/non-marine
        if(processed.habitat!=null){
        	
        	//retrieve the species profile
        	val taxonProfile = TaxonProfileDAO.getByGuid(processedClassification.taxonConceptID)
        	if(!taxonProfile.isEmpty && taxonProfile.get.habitats!=null && taxonProfile.get.habitats.size>0){
        		val habitatsAsString =  taxonProfile.get.habitats.reduceLeft(_+","+_)
        		val habitatFromPoint = processed.habitat
        		val habitatsForSpecies = taxonProfile.get.habitats
        		//is "terrestrial" the same as "non-marine" ??
        		val validHabitat = HabitatMap.areTermsCompatible(habitatFromPoint, habitatsForSpecies)
        		if(!validHabitat.isEmpty){
        			if(validHabitat.get){
        				println("[QualityAssertion] Habitats compatible" + rawOccurrence.uuid + ", processed:" + processed.habitat + ", retrieved:" + habitatsAsString)
        			} else {
        				println("[QualityAssertion] ******** Habitats incompatible for UUID: " + rawOccurrence.uuid + ", processed:" + processed.habitat + ", retrieved:" + habitatsAsString)
        				var qa = new QualityAssertion
        				qa.userId = "system"
        				qa.assertionCode = AssertionCodes.COORDINATE_HABITAT_MISMATCH.code
        				qa.comment = "Recognised habitats for species: " + habitatsAsString+", Value determined from coordinates: "+habitatFromPoint
        				qa.positive = false
        				OccurrenceDAO.addQualityAssertion(rawOccurrence.uuid, qa, AssertionCodes.COORDINATE_HABITAT_MISMATCH)
        			}
        		}
        	}
        }
        
        //check centre point of the state


      }
    }
  }

  /**
   * Match the classification
   */
  def processClassification(rawOccurrence: Occurrence, raw: Classification, processed: Classification) {
    val classification = new LinnaeanRankClassification(
      raw.kingdom,
      raw.phylum,
      raw.classs,
      raw.order,
      raw.family,
      raw.genus,
      raw.species,
      raw.specificEpithet,
      raw.subspecies,
      raw.infraspecificEpithet,
      raw.scientificName)
    //println("Record: "+occ.uuid+", classification for Kingdom: "+occ.kingdom+", Family:"+  occ.family +", Genus:"+  occ.genus +", Species: " +occ.species+", Epithet: " +occ.specificEpithet)
    try {
      val nsr = DAO.nameIndex.searchForRecord(classification, true)
      //store the matched classification
      if (nsr != null) {
        val classification = nsr.getRankClassification
        //store ".p" values
        processed.kingdom = classification.getKingdom
        processed.phylum = classification.getPhylum
        processed.classs = classification.getKlass
        processed.order = classification.getOrder
        processed.family = classification.getFamily
        processed.genus = classification.getGenus
        processed.species = classification.getSpecies
        processed.specificEpithet = classification.getSpecificEpithet
        processed.scientificName = classification.getScientificName
        processed.taxonConceptID = nsr.getLsid
      } else {
        println("[QualityAssertion] No match for record, classification for Kingdom: " + raw.kingdom + ", Family:" + raw.family + ", Genus:" + raw.genus + ", Species: " + raw.species + ", Epithet: " + raw.specificEpithet)
      }
    } catch {
      case e: HomonymException => //println("Homonym exception for record, classification for Kingdom: "+raw.kingdom+", Family:"+  raw.family +", Genus:"+  raw.genus +", Species: " +raw.species+", Epithet: " +raw.specificEpithet)
      case e: Exception => e.printStackTrace
    }
  }
}