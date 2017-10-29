package au.org.ala.biocache.processor

import au.org.ala.biocache.Config
import au.org.ala.biocache.caches.{LocationDAO, SpatialLayerDAO}
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.model.{FullRecord, QualityAssertion, Versions}
import au.org.ala.biocache.util.{GridUtil, Json}
import au.org.ala.biocache.vocab.StateProvinces
import au.org.ala.sds.SensitiveDataService
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

/**
  * Performs sensitive data processing on the record.
  * Where required this will reduce the quality of location and event information.
  */
class SensitivityProcessor extends Processor {

  val logger = LoggerFactory.getLogger("SensitivityProcessor")

  import JavaConversions._

  //This is being initialised here because it may take some time to load all the XML records...
  val sds = new SensitiveDataService()

  def getName = "sensitive"

  /**
    * Process the supplied record.
    *
    * @param guid
    * @param raw
    * @param processed
    * @param lastProcessed
    * @return
    */
  def process(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None) :
  Array[QualityAssertion] = {

    // if SDS disabled, do nothing
    if(!Config.sdsEnabled){
      return Array()
    }

    val exact = getExactSciName(raw)

    //is the name recognised as sensitive?
    if(!sds.isTaxonSensitive(Config.sdsFinder, exact, processed.classification.taxonConceptID)){
      return Array()
    }

    //needs to be performed for all records whether or not they are in Australia
    //get a map representation of the raw record for sdsFlag fields...
    val rawMap = scala.collection.mutable.Map[String, String]()
    rawMap.putAll(raw.getRawFields())
    if (rawMap.isEmpty) {
      //populate rawMap if raw.rawFields is empty
      raw.objectArray.foreach { poso =>
        val map = FullRecordMapper.mapObjectToProperties(poso, Versions.RAW)
        rawMap ++= map
      }
    }


    //use the processed versions of the coordinates for the sensitivity check if raw not available
    //this would be the case when coordinates have been derived from easting/northings or grid references
    if(!raw.location.hasCoordinates && processed.location.hasCoordinates){
      rawMap.put("decimalLatitude", processed.location.decimalLatitude)
      rawMap.put("decimalLongitude", processed.location.decimalLongitude)
      rawMap.put("coordinatePrecision", processed.location.coordinatePrecision)
      rawMap.put("coordinateUncertaintyInMeters", processed.location.coordinateUncertaintyInMeters)
    }

    if(processed.location.hasCoordinates){

      //do a dynamic lookup for the layers required for the SDS
      val layerIntersect = SpatialLayerDAO.intersect(
        processed.location.decimalLongitude.toDouble,
        processed.location.decimalLatitude.toDouble)

      SpatialLayerDAO.sdsLayerList.foreach { key =>
        rawMap.put(key, layerIntersect.getOrElse(key, "n/a"))
      }

      val intersectStateProvince = layerIntersect.getOrElse(Config.stateProvinceLayerID, "")

      //reset
      if(StringUtils.isBlank(intersectStateProvince)){
        val stringMatchState = StateProvinces.matchTerm(raw.location.stateProvince)
        if(!stringMatchState.isEmpty){
          rawMap.put("stateProvince", stringMatchState.get.canonical)
        }
      } else {
        rawMap.put("stateProvince", intersectStateProvince)
      }
    }

    //this flag stops on the fly sampling being performed by SDS
    rawMap.put(SensitiveDataService.SAMPLED_VALUES_PROVIDED, "true")

    //put the processed event date components in to allow for correct date applications of the rules
    if(processed.event.day != null)
      rawMap("day") = processed.event.day
    if(processed.event.month != null)
      rawMap("month") = processed.event.month
    if(processed.event.year != null)
      rawMap("year") = processed.event.year

    //SDS check - now get the ValidationOutcome from the Sensitive Data Service
    val outcome = sds.testMapDetails(Config.sdsFinder, rawMap, exact, processed.classification.taxonConceptID)

    logger.debug("SDS outcome: " + outcome)

    /************** SDS check end ************/

    if (outcome != null && outcome.isValid && outcome.isSensitive) {

      logger.debug("Taxon identified as sensitive.....")
      if (outcome.getResult != null) {

        val map: scala.collection.mutable.Map[String, Object] = outcome.getResult

        //convert it to a string string map
        val stringMap = map.collect {
          case (key, value) if value != null => if (key == "originalSensitiveValues") {
            val osv = value.asInstanceOf[java.util.HashMap[String, String]]
            // add the original "processed" coordinate uncertainty to the sensitive values so that it
            // can be available if necessary
            if (processed.location.coordinateUncertaintyInMeters != null) {
              osv.put("coordinateUncertaintyInMeters.p", processed.location.coordinateUncertaintyInMeters)
            }
            if(raw.location.gridReference != null && raw.location.gridReference != ""){
              osv.put("gridReference", raw.location.gridReference)
            }

            //remove all the el/cl's from the original sensitive values
            SpatialLayerDAO.sdsLayerList.foreach { key => osv.remove(key) }
            val newv = Json.toJSON(osv)
            (key -> newv)
          } else {
            (key -> value.toString)
          }
        }

        //take away the values that need to be added to the processed record NOT the raw record
        val uncertainty = stringMap.get("generalisationInMetres")
        val generalisationToApplyInMetres = stringMap.get("generalisationToApplyInMetres")
        if (!uncertainty.isEmpty) {
          //we know that we have sensitised, add the uncertainty to the currently processed uncertainty
          if (StringUtils.isNotEmpty(uncertainty.get.toString)) {

            val currentUncertainty = if (StringUtils.isNotEmpty(processed.location.coordinateUncertaintyInMeters)) {
              java.lang.Float.parseFloat(processed.location.coordinateUncertaintyInMeters)
            } else {
              0
            }

            val newUncertainty = currentUncertainty + java.lang.Integer.parseInt(uncertainty.get.toString)
            processed.location.coordinateUncertaintyInMeters = newUncertainty.toString

          }
          processed.location.decimalLatitude = stringMap.getOrElse("decimalLatitude", "")
          processed.location.decimalLongitude = stringMap.getOrElse("decimalLongitude", "")
          processed.location.northing = ""
          processed.location.easting = ""
          processed.location.bbox = ""
          stringMap -= "generalisationInMetres"
        }

        //remove other GIS references
        if(Config.gridRefIndexingEnabled && raw.location.gridReference != null){

          if (generalisationToApplyInMetres.isDefined) {
            //reduce the quality of the grid reference
            if (generalisationToApplyInMetres.get == null || generalisationToApplyInMetres.get == "") {
              stringMap.put("gridReference", "")
            } else {
              processed.location.coordinateUncertaintyInMeters = generalisationToApplyInMetres.get
              val generalisedRef = GridUtil.convertReferenceToResolution(raw.location.gridReference, generalisationToApplyInMetres.get)
              if (generalisedRef.isDefined) {
                stringMap.put("gridReference", generalisedRef.get)
              } else {
                stringMap.put("gridReference", "")
              }
            }
          } else {
            stringMap.put("gridReference", "")
          }
        }

        processed.occurrence.informationWithheld = stringMap.getOrElse("informationWithheld", "")
        processed.occurrence.dataGeneralizations = stringMap.getOrElse("dataGeneralizations", "")
        stringMap -= "informationWithheld"
        stringMap -= "dataGeneralizations"

        //remove the day from the values if present
        raw.event.day = ""
        processed.event.day = ""
        processed.event.eventDate = ""
        if (processed.event.eventDateEnd != null) {
          processed.event.eventDateEnd = ""
        }

        stringMap.put("easting", "")
        stringMap.put("northing", "")

        //update the raw record with whatever is left in the stringMap - change to use DAO method...
        if(StringUtils.isNotBlank(raw.rowKey)){
          Config.persistenceManager.put(raw.rowKey, "occ", stringMap.toMap, false)
          try {
            if(StringUtils.isNotBlank(processed.location.decimalLatitude) && 
               StringUtils.isNotBlank(processed.location.decimalLongitude)) {
              LocationDAO.storePointForSampling(processed.location.decimalLatitude, processed.location.decimalLongitude)
            }
          } catch {
            case e: Exception => {
              logger.error("Error storing point for sampling for SDS record: " + raw.rowKey + " " + processed.uuid, e)
            }
          }
        }

      } else if (!outcome.isLoadable() && Config.obeySDSIsLoadable){
        logger.warn("SDS isLoadable status is currently not being used. Would apply to: " + processed.uuid)
      }

      if(outcome.getReport().getMessages() != null){
        var infoMessage = ""
        outcome.getReport().getMessages().foreach(message => {
          infoMessage += message.getCategory() + "\t" + message.getMessageText() + "\n"
        })
        processed.occurrence.informationWithheld = infoMessage
      }
    } else {
      //Species is NOT sensitive
      //if the raw record has originalSensitive values we need to re-initialise the value
      if (StringUtils.isNotBlank(raw.rowKey) &&
        raw.occurrence.originalSensitiveValues != null &&
        !raw.occurrence.originalSensitiveValues.isEmpty) {
        Config.persistenceManager.put(raw.rowKey, "occ", raw.occurrence.originalSensitiveValues + ("originalSensitiveValues" -> ""), false)
      }
    }

    Array()
  }

  /**
   * Retrieve an scientific name to use for SDS processing.
   *
   * @param raw
   * @return
   */
  private def getExactSciName(raw: FullRecord) : String = {
    if (raw.classification.scientificName != null)
      raw.classification.scientificName
    else if (raw.classification.subspecies != null)
      raw.classification.subspecies
    else if (raw.classification.species != null)
      raw.classification.species
    else if (raw.classification.genus != null) {
      if (raw.classification.specificEpithet != null) {
        if (raw.classification.infraspecificEpithet != null)
          raw.classification.genus + " " + raw.classification.specificEpithet + " " + raw.classification.infraspecificEpithet
        else
          raw.classification.genus + " " + raw.classification.specificEpithet
      } else {
        raw.classification.genus
      }
    }
    else if (raw.classification.vernacularName != null) // handle the case where only a common name is provided.
      raw.classification.vernacularName
    else //return the name default name string which will be null
      raw.classification.scientificName
  }

  def skip(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {
    val assertions = new ArrayBuffer[QualityAssertion]

    //get the data resource information to check if it has mapped collections
    if (lastProcessed.isDefined) {
      //no assertions
      //assertions ++= lastProcessed.get.findAssertions(Array())

      //update the details from lastProcessed
      processed.location.coordinateUncertaintyInMeters = lastProcessed.get.location.coordinateUncertaintyInMeters
      processed.location.decimalLatitude = lastProcessed.get.location.decimalLatitude
      processed.location.decimalLongitude = lastProcessed.get.location.decimalLongitude
      processed.location.northing = lastProcessed.get.location.northing
      processed.location.easting = lastProcessed.get.location.easting
      processed.location.bbox = lastProcessed.get.location.bbox
      processed.occurrence.informationWithheld = lastProcessed.get.occurrence.informationWithheld
      processed.occurrence.dataGeneralizations = lastProcessed.get.occurrence.dataGeneralizations
      processed.event.day = lastProcessed.get.event.day
      processed.event.eventDate = lastProcessed.get.event.eventDate
      processed.event.eventDateEnd = lastProcessed.get.event.eventDateEnd
    }

    assertions.toArray
  }
}
