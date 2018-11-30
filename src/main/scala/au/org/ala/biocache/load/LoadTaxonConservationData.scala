package au.org.ala.biocache.load

import au.org.ala.biocache.Config
import au.org.ala.biocache.caches.TaxonProfileDAO
import au.org.ala.biocache.index.Counter
import au.org.ala.biocache.persistence.Cassandra3PersistenceManager
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * A class that can be used to reload taxon conservation values for all records.
  *
  * @param centralCounter
  * @param threadId
  */
class LoadTaxonConservationData(centralCounter: Counter, threadId: Int) extends Runnable {

  val logger = LoggerFactory.getLogger("LoadTaxonConservationData")
  var ids = 0
  val threads = 2
  var batches = 0

  def run {
    var counter = 0
    val start = System.currentTimeMillis
    val sep = Config.persistenceManager.fieldDelimiter

    val batch: mutable.Map[String, Map[String, String]] = mutable.Map[String, Map[String, String]]()

    Config.persistenceManager.pageOverLocal("occ", (guid, map, _) => {
      val updates = mutable.Map[String, String]()
      val taxonProfileWithOption = TaxonProfileDAO.getByGuid(map.getOrElse("taxonConceptID" + sep + "p", ""))
      if (!taxonProfileWithOption.isEmpty) {
        val taxonProfile = taxonProfileWithOption.get
        //add the conservation status if necessary
        if (taxonProfile.conservation != null) {
          val country = taxonProfile.retrieveConservationStatus(map.getOrElse("country" + sep + "p", ""))
          updates.put("countryConservation" + sep + "p", country.getOrElse(""))
          val state = taxonProfile.retrieveConservationStatus(map.getOrElse("stateProvince" + sep + "p", ""))
          updates.put("stateConservation" + sep + "p", state.getOrElse(""))
          val global = taxonProfile.retrieveConservationStatus("global")
          updates.put("global", global.getOrElse(""))
        }
      }
      if (updates.size < 3) {
        updates.put("countryConservation" + sep + "p", "")
        updates.put("stateConservation" + sep + "p", "")
        updates.put("global", "")
      }
      val changes = updates.filter(it => map.getOrElse(it._1, "") != it._2)
      if (!changes.isEmpty) {
        batch.put(guid, updates.toMap)
      }

      counter += 1
      if (counter % 10000 == 0) {
        logger.info("[LoadTaxonConservationData Thread " + threadId + "] Import of sample data " + counter + " Last key " + guid)

        if (!batch.isEmpty) {
          logger.info("writing")
          Config.persistenceManager.putBatch("occ", batch.toMap, true, false)
          batch.clear()
        }
      }

      true
    }, 4, Array("taxonConceptID" + sep + "p", "country" + sep + "p", "countryConservation" + sep + "p", "stateProvince" + sep + "p", "stateConservation" + sep + "p", "global"))

    if (!batch.isEmpty) {
      logger.info("writing")
      Config.persistenceManager.putBatch("occ", batch.toMap, true, false)
      batch.clear()
    }

    val fin = System.currentTimeMillis
    logger.info("[LoadTaxonConservationData Thread " + threadId + "] " + counter + " took " + ((fin - start).toFloat) / 1000f + " seconds")
    logger.info("Finished.")
  }
}