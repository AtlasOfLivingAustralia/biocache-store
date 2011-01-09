package au.org.ala.util

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
import au.org.ala.checklist.lucene.HomonymException
import au.org.ala.data.util.RankType
import au.org.ala.biocache.Occurrence
import au.org.ala.data.model.LinnaeanRankClassification
import au.org.ala.checklist.lucene.CBIndexSearch
import au.org.ala.biocache.OccurrenceType
import au.org.ala.biocache.OccurrenceDAO
object ProcessRecords {
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
  def main(args: Array[String]): Unit = { 
	  
	  val odao = new OccurrenceDAO
	  val pdao = new LocationDAO
	  var start = System.currentTimeMillis
	  var finish = System.currentTimeMillis
	  var counter = 0
	  var startTime = System.currentTimeMillis
      var finishTime = System.currentTimeMillis
	   
	  //page over all records and process
	  odao.pageOverAll(OccurrenceType.Raw, o => {
	 	  counter += 1
	 	  if(!o.isEmpty){
	 	 	  
	 	 	  val rawOccurrence = o.get._1 
	 	 	  val rawClassification = o.get._2
	 	 	  val rawLocation = o.get._3
	 	 	  val rawEvent = o.get._4

	 	 	  var processedOccurrence = rawOccurrence.clone
	 	 	  var processedClassification = new Classification
	 	 	  var processedLocation = rawLocation.clone
	 	 	  var processedEvent = rawEvent.clone
	 	 	  
	 	 	  if(counter % 1000 == 0) { 
	 	 	 	  finishTime = System.currentTimeMillis
	 	 	 	  println(counter + " >> Last key : "+rawOccurrence.uuid +", records per sec: " + 1000f / (((finishTime - startTime).toFloat) / 1000f))
	 	 	 	  startTime = System.currentTimeMillis
	 	 	  }
	 	 	  
	 	 	  //find a classification in NSLs
	 	 	  processClassification(rawOccurrence, rawClassification, processedClassification)

			  //perform gazetteer lookups - just using point hash for now
	 	 	  processLocation(rawOccurrence, rawLocation, processedLocation, pdao, odao) 
	 	 	  
	 	 	  //temporal processing
	 	 	  processEvent(rawOccurrence, rawEvent, processedEvent, odao)
	 	 	  
	 	 	  //basis of record parsing
	 	 	  processBasisOfRecord(rawOccurrence, processedOccurrence, odao)
	 	 	  
	 	 	  //type status normalisation
	 	 	  processTypeStatus(rawOccurrence, processedOccurrence, odao)
	 	 	  
	 	 	  //process the attribution - call out to the Collectory...
	 	 	  
	 	 	  
	 	 	  //BIE properties lookup - use AVRO
	 	 	  
			  //perform SDS lookups - retrieve from BIE for now....
			  
	 	 	  //store the occurrence
 	 		  odao.updateOccurrence(rawOccurrence.uuid, processedOccurrence, OccurrenceType.Processed)
 	 		  odao.updateOccurrence(rawOccurrence.uuid, processedLocation, OccurrenceType.Processed)
 	 		  odao.updateOccurrence(rawOccurrence.uuid, processedClassification, OccurrenceType.Processed)
 	 		  odao.updateOccurrence(rawOccurrence.uuid, processedEvent, OccurrenceType.Processed)
	 	  }
	  })
	  Pelops.shutdown
  }

  def processEvent(rawOccurrence:Occurrence, rawEvent:Event, processEvent:Event, odao:OccurrenceDAO){
	  
	  //fields to check
//  @BeanProperty var day:String = _
//  @BeanProperty var endDayOfYear:String = _
//  @BeanProperty var eventAttributes:String = _
//  @BeanProperty var eventDate:String = _
//  @BeanProperty var eventID:String = _
//  @BeanProperty var eventRemarks:String = _
//  @BeanProperty var eventTime:String = _
//  @BeanProperty var verbatimEventDate:String = _
//  @BeanProperty var year:String = _
//  @BeanProperty var month:String = _
//  @BeanProperty var startDayOfYear:String = _
//  //custom date range fields
//  @BeanProperty var startYear:String = _
//  @BeanProperty var endYear:String = _	  
	  //
	  
	  //need to populate the eventDate, day, month and year
	  
  }
  
  
  def processTypeStatus(rawOccurrence:Occurrence, processedOccurrence:Occurrence, odao:OccurrenceDAO){
	  
	  if(rawOccurrence.typeStatus != null && rawOccurrence.typeStatus.isEmpty){
		  val term = TypeStatus.matchTerm(rawOccurrence.typeStatus)
		  if(term.isEmpty){
		 	  //add a quality assertion
		 	  val qa = new QualityAssertion
		 	  qa.positive = false
		 	  qa.assertionCode  = AssertionCodes.OTHER_UNRECOGNISED_TYPESTATUS 
		 	  qa.comment = "Unrecognised type status"
		 	  qa.userId = "system"
		 	  odao.addQualityAssertion(rawOccurrence.uuid, qa)
		  } else {
		 	  processedOccurrence.basisOfRecord = term.get.canonical
		  }
	  }
  }
  
  /**
   * Process basis of record
   */
  def processBasisOfRecord(rawOccurrence:Occurrence, processedOccurrence:Occurrence, odao:OccurrenceDAO){
	  
	  if(rawOccurrence.basisOfRecord == null || rawOccurrence.basisOfRecord.isEmpty){
	 	  //add a quality assertion
	 	  val qa = new QualityAssertion
	 	  qa.positive = false
	 	  qa.assertionCode  = AssertionCodes.OTHER_MISSING_BASIS_OF_RECORD
	 	  qa.comment = "Missing basis of record"
	 	  qa.userId = "system"
	 	  odao.addQualityAssertion(rawOccurrence.uuid, qa)
	  } else {
		  val term = BasisOfRecord.matchTerm(rawOccurrence.basisOfRecord)
		  if(term.isEmpty){
		 	  //add a quality assertion
		 	  println("[QualityAssertion] "+rawOccurrence.uuid+", unrecognised BoR: "+rawOccurrence.uuid+", BoR:"+rawOccurrence.basisOfRecord)
		 	  val qa = new QualityAssertion
		 	  qa.positive = false
		 	  qa.assertionCode  = AssertionCodes.OTHER_BADLY_FORMED_BASIS_OF_RECORD 
		 	  qa.comment = "Unrecognised basis of record"
		 	  qa.userId = "system"
		 	  odao.addQualityAssertion(rawOccurrence.uuid, qa)
		  } else {
		 	  processedOccurrence.basisOfRecord = term.get.canonical
		  }
	  }
  }
  
  /**
   * Process geospatial details
   */
  def processLocation(rawOccurrence:Occurrence, raw:Location, processed:Location, pdao:LocationDAO, odao:OccurrenceDAO) {
	  //retrieve the point
	  if(raw.decimalLatitude!=null && raw.decimalLongitude!=null){
	 	  
	 	  //TODO validate decimal degrees
 	 	  processed.decimalLatitude = raw.decimalLatitude
 	 	  processed.decimalLongitude = raw.decimalLongitude
 	 	  
 	 	  //validate coordinate accuracy (coordinateUncertaintyInMeters) and coordinatePrecision (precision - A. Chapman)
 	 	  
 	 	  //
 	 	  
	 	  
	 	  //generate coordinate accuracy if not supplied
	 	  
	 	  
	 	  val point = pdao.getLocationByLatLon(raw.decimalLatitude, raw.decimalLongitude);
	 	  if(!point.isEmpty){
	 	 	  
	 	 	  //add state information
	 	 	  processed.stateProvince = point.get.stateProvince
	 	 	  processed.ibra = point.get.ibra
	 	 	  processed.imcra = point.get.imcra
	 	 	  processed.lga = point.get.lga
	 	 	  
	 	 	  //check matched stateProvince
	 	 	  if(processed.stateProvince!=null && raw.stateProvince!=null){
	 	 		  //quality assertions
	 	 	 	  val stateTerm = States.matchTerm(raw.stateProvince)
	 	 	 	  
	 	 		  if(!stateTerm.isEmpty && !processed.stateProvince.equalsIgnoreCase(stateTerm.get.canonical)){
	 	 		 	  println("[QualityAssertion] "+rawOccurrence.uuid+", processed:"+processed.stateProvince+", raw:"+raw.stateProvince)
	 	 		 	  //add a quality assertion
	 	 		 	  val qa = new QualityAssertion
	 	 		 	  qa.positive = false
	 	 		 	  qa.assertionCode  = AssertionCodes.GEOSPATIAL_STATE_COORDINATE_MISMATCH 
	 	 		 	  qa.comment = "Supplied: " + stateTerm.get.canonical + ", Calculated: "+ processed.stateProvince
	 	 		 	  qa.userId = "system"
	 	 		 	  //store the assertion
	 	 		      odao.addQualityAssertion(rawOccurrence.uuid, qa);
	 	 		  }
	 	 	  }
	 	 	  
	 	 	  //check marine/non-marine
	 	 	   
	 	 	  //check centre point of the state
	 	 	  
	 	 	  //check 
	 	 	   
	 	  }
	  }
  }
  
  /**
   * Match the classification
   */
  def processClassification(rawOccurrence:Occurrence, raw:Classification, processed:Classification) {
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
 	  try{
 	 	  val nsr = DAO.nameIndex.searchForRecord(classification, true)
 	 	  //store the matched classification
 	 	  if(nsr!=null){
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
 	 	 	  println("[QualityAssertion] No match for record, classification for Kingdom: "+raw.kingdom+", Family:"+  raw.family +", Genus:"+  raw.genus +", Species: " +raw.species+", Epithet: " +raw.specificEpithet)
 	 	  }
 	  } catch {
 	 	  case e:HomonymException => //println("Homonym exception for record, classification for Kingdom: "+raw.kingdom+", Family:"+  raw.family +", Genus:"+  raw.genus +", Species: " +raw.species+", Epithet: " +raw.specificEpithet)
 	 	  case e:Exception => e.printStackTrace
 	  }
  }
}