package au.org.ala.biocache.caches

import au.org.ala.biocache.Config
import au.org.ala.sds.SensitiveDataService
import com.google.common.cache.CacheBuilder

object SensitivityDAO {

  //This is being initialised here because it may take some time to load all the XML records...
  val sds = new SensitiveDataService()

  val lruSensitiveLookups = CacheBuilder.newBuilder().maximumSize(Config.sensitivityCacheSize).build[String, String]()

  def isSensitive(exact:String, taxonConceptID:String) : Boolean = {

    val hashKey = exact + "|" + taxonConceptID
    val isSensitiveString = lruSensitiveLookups.getIfPresent(hashKey)
    val isSensitive = {
      if (isSensitiveString == null) {
        val isSensitive = sds.isTaxonSensitive(Config.sdsFinder, exact, taxonConceptID)
        lruSensitiveLookups.put(hashKey, isSensitive.toString())
        isSensitive
      } else {
        isSensitiveString.toBoolean
      }
    }

    isSensitive
  }

  def getSDS = sds

  def getCacheSize = lruSensitiveLookups.size().toInt
}
