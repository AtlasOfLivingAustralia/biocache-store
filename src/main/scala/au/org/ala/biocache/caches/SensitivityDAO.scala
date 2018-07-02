package au.org.ala.biocache.caches

import au.org.ala.biocache.Config
import au.org.ala.sds.SensitiveDataService
import com.google.common.cache.CacheBuilder

object SensitivityDAO {

  //This is being initialised here because it may take some time to load all the XML records...
  val sds = new SensitiveDataService()

  val lruSensitiveLookups = CacheBuilder.newBuilder().maximumSize(Config.sensitivityCacheSize).build[String, java.lang.Boolean]()

  def isSensitive(exact:String, taxonConceptID:String) : Boolean = {

    val hashKey = exact + "|" + taxonConceptID
    val isSensitiveCached = lruSensitiveLookups.getIfPresent(hashKey)
    if (isSensitiveCached == null) {
      val isSensitive = sds.isTaxonSensitive(Config.sdsFinder, exact, taxonConceptID)
      addToCache(exact, taxonConceptID, isSensitive)
      isSensitive
    } else {
      isSensitiveCached
    }
  }

  def addToCache(exact:String, taxonConceptID:String, isSensitive:Boolean): Unit ={
    val hashKey = exact + "|" + taxonConceptID
    lruSensitiveLookups.put(hashKey, isSensitive)
  }

  def getSDS = sds

  def getCacheSize = lruSensitiveLookups.size().toInt
}
