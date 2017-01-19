package au.org.ala.biocache.caches

import au.org.ala.biocache.Config
import au.org.ala.names.model.{NameSearchResult, MetricsResultDTO, LinnaeanRankClassification}
import au.org.ala.biocache.model.Classification
import org.slf4j.LoggerFactory

/**
 * A DAO for accessing classification information in the cache. If the
 * value does not exist in the cache the name matching API is called.
 *
 * The cache will store a classification object for names that match. If the
 * name causes a homonym exception or is not found the ErrorCode is stored.
 *
 * @author Natasha Carter
 */
object ClassificationDAO {

  val logger = LoggerFactory.getLogger("ClassificationDAO")
  private val lru = new org.apache.commons.collections.map.LRUMap(10000)
  private val lock : AnyRef = new Object()
  private val nameIndex = Config.nameIndex

  /** Limit on the number of recursive lookups to avoid stackoverflow */
  private val RECURSIVE_LOOP_LIMIT = 4

  private def stripStrayQuotes(str:String) : String  = {
    if (str == null){
      null
    } else {
      var normalised = str
      if(normalised.startsWith("'") || normalised.startsWith("\"")) normalised = normalised.drop(1)
      if(normalised.endsWith("'") || normalised.endsWith("\"")) normalised = normalised.dropRight(1)
      normalised
    }
  }

  /**
   * Uses a LRU cache of classification lookups.
   */
  def get(cl:Classification, count:Int=0) : Option[MetricsResultDTO] = {

    //use the vernacular name to lookup if there is no scientific name or higher level classification
    //we don't really trust vernacular name matches thus only use as a last resort
    val hash = {
      if(cl.vernacularName == null || cl.scientificName != null || cl.specificEpithet != null
        || cl.infraspecificEpithet != null || cl.kingdom != null || cl.phylum != null
        || cl.classs != null || cl.order != null || cl.family !=null  || cl.genus!=null
        || cl.taxonConceptID != null || cl.taxonID != null
      ){
        Array(cl.kingdom,cl.phylum,cl.classs,cl.order,cl.family,cl.genus,cl.species,cl.specificEpithet,
          cl.subspecies,cl.infraspecificEpithet,cl.scientificName,cl.taxonRank,cl.taxonConceptID,cl.taxonID).reduceLeft(_+"|"+_)
      } else {
        cl.vernacularName
      }
    }

    //set the scientificName using available elements of the higher classification
    if (cl.scientificName == null){
      cl.scientificName = if (cl.subspecies != null) {
        cl.subspecies
      } else if (cl.genus != null && cl.specificEpithet != null && cl.infraspecificEpithet != null) {
        cl.genus + " " + cl.specificEpithet + " " + cl.infraspecificEpithet
      } else if (cl.genus != null && cl.specificEpithet != null ) {
        cl.genus + " " + cl.specificEpithet
      } else if (cl.species != null) {
        cl.species
      } else if (cl.genus != null) {
        cl.genus
      } else if (cl.family != null) {
        cl.family
      } else if (cl.classs != null) {
        cl.classs
      } else if (cl.order != null) {
        cl.order
      } else if (cl.phylum != null) {
        cl.phylum
      } else if (cl.kingdom != null) {
        cl.kingdom
      } else {
        null
      }
    }

    val cachedObject = lock.synchronized { lru.get(hash) }

    if(cachedObject != null){
      cachedObject.asInstanceOf[Option[MetricsResultDTO]]
    } else {

      //attempt 1: search via taxonConceptID or taxonID if provided
      val idnsr = if(cl.taxonConceptID != null) {
        nameIndex.searchForRecordByLsid(cl.taxonConceptID)
      } else if (cl.taxonID != null) {
        nameIndex.searchForRecordByLsid(cl.taxonID)
      } else {
        null
      }

      //attempt 2: search using the taxonomic classification if provided
      var resultMetric = {
        try {
          if(idnsr != null){
            val metric = new MetricsResultDTO
            metric.setResult(idnsr)
            metric
          } else if(hash.contains("|")) {
            val lrcl = new LinnaeanRankClassification(
              stripStrayQuotes(cl.kingdom),
              stripStrayQuotes(cl.phylum),
              stripStrayQuotes(cl.classs),
              stripStrayQuotes(cl.order),
              stripStrayQuotes(cl.family),
              stripStrayQuotes(cl.genus),
              stripStrayQuotes(cl.species),
              stripStrayQuotes(cl.specificEpithet),
              stripStrayQuotes(cl.subspecies),
              stripStrayQuotes(cl.infraspecificEpithet),
              stripStrayQuotes(cl.scientificName))
            lrcl.setRank(cl.taxonRank)
            nameIndex.searchForRecordMetrics(lrcl, true, true)
            //fuzzy matching is enabled because we have taxonomic hints to help prevent dodgy matches
          } else {
            null
          }
        } catch {
          case e:Exception => {
            logger.debug(e.getMessage + ", hash =  " + hash, e)
          }
          null
        }
      }

      //attempt 3: last resort, search using  vernacular name
      if(resultMetric == null) {
        val cnsr = nameIndex.searchForCommonName(cl.getVernacularName)
        if(cnsr != null){
          resultMetric = new MetricsResultDTO
          resultMetric.setResult(cnsr)
        }
      }

      // if we have result
      if(resultMetric != null && resultMetric.getResult() != null){

        //handle the case where the species is a synonym this is a temporary fix should probably go in ala-name-matching
        var result:Option[MetricsResultDTO] = if(resultMetric.getResult.isSynonym){
          val ansr = nameIndex.searchForRecordByLsid(resultMetric.getResult.getAcceptedLsid)
          if(ansr != null){
            //change the name match metric for a synonym
            ansr.setMatchType(resultMetric.getResult.getMatchType())
            resultMetric.setResult(ansr)
            Some(resultMetric)
          } else {
            None
          }
        } else {
          Some(resultMetric)
        }

        if(result.isDefined){
          //update the subspecies or below value if necessary
          val rank = result.get.getResult.getRank
          //7000 is the rank ID for species
          if(rank != null && rank.getId() > 7000 && rank.getId < 9999){
            result.get.getResult.getRankClassification.setSubspecies(result.get.getResult.getRankClassification.getScientificName())
          }
        } else {
          logger.debug("Unable to locate accepted concept for synonym " + resultMetric.getResult + ". Attempting a higher level match")
          if((cl.kingdom != null || cl.phylum != null || cl.classs != null || cl.order != null || cl.family != null
            || cl.genus != null) && (cl.getScientificName() != null || cl.getSpecies() != null
            || cl.getSpecificEpithet() != null || cl.getInfraspecificEpithet() != null)){

            val newcl = cl.clone()
            newcl.setScientificName(null)
            newcl.setInfraspecificEpithet(null)
            newcl.setSpecificEpithet(null)
            newcl.setSpecies(null)
            updateClassificationRemovingMissingSynonym(newcl, resultMetric.getResult())

            if(count < RECURSIVE_LOOP_LIMIT){
              result = get(newcl, count + 1)
            } else {
              logger.warn("Potential recursive issue with " + cl.getKingdom() + " " + cl.getPhylum + " " +
                cl.getClasss + " " + cl.getOrder + " " + cl.getFamily)
            }
          } else {
            logger.warn("Recursively unable to locate a synonym for " + cl.scientificName)
          }
        }
        lock.synchronized { lru.put(hash, result) }
        result
      } else {
        val result = if(resultMetric != null) {
          Some(resultMetric)
        } else {
          None
        }
        lock.synchronized { lru.put(hash, result) }
        result
      }
    }
  }

  private def updateClassificationRemovingMissingSynonym(newcl:Classification, result:NameSearchResult){
    val sciName = result.getRankClassification().getScientificName()
    if(newcl.genus == sciName)
      newcl.genus = null
    if(newcl.family == sciName)
      newcl.family = null
    if(newcl.order == sciName)
      newcl.order = null
    if(newcl.classs == sciName)
      newcl.classs = null
    if(newcl.phylum == sciName)
      newcl.phylum = null
  }
}