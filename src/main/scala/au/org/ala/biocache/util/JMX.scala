package au.org.ala.biocache.util

import java.lang.management.ManagementFactory
import javax.management.ObjectName

/**
  * Singleton interface to JMX.
  */
object JMX {

  val mbs = ManagementFactory.getPlatformMBeanServer()

  //indexing status
  val indexStatus = new IndexStatus()
  mbs.registerMBean(indexStatus, new ObjectName("au.org.ala.biocache:type=Indexing"))

  //processing status
  val processingStatus = new ProcessingStatus()
  mbs.registerMBean( processingStatus, new ObjectName("au.org.ala.biocache:type=Processing"))

  def updateIndexStatus(totalRecords:Long,
                        recordsPerSec: Float,
                        cassandraTime:Long,
                        processingTime:Long,
                        solrTime:Long,
                        totalTime:Long,
                        indexDocsCommitted:Long,
                        indexDocsInRam:Long,
                        indexDocsInRamMB:Long,
                        processingQueue:Long,
                        luceneQueue:Long,
                        commitBatchQueue:Long
                       ): Unit = {
    indexStatus.totalRecords = totalRecords
    indexStatus.recordsPerSec = recordsPerSec
    indexStatus.cassandraTime = cassandraTime
    indexStatus.processingTime = processingTime
    indexStatus.solrTime = solrTime
    indexStatus.totalTime = totalTime
    indexStatus.indexDocsCommitted = indexDocsCommitted
    indexStatus.indexDocsInRam = indexDocsInRam
    indexStatus.indexDocsInRamMB  = indexDocsInRamMB
    indexStatus.processingQueue = processingQueue
    indexStatus.luceneQueue = luceneQueue
    indexStatus.commitBatchQueue = commitBatchQueue
  }

  def updateProcessingStats(recordsPerSec: Float,
                             lastPageInSecs: Float,
                             totalRecordsRead: Int,
                             totalRecordsUpdated: Int
                           ): Unit = {
    processingStatus.recordsPerSec = recordsPerSec
    processingStatus.lastPageInSecs = lastPageInSecs
    processingStatus.recordsRead = totalRecordsRead
    processingStatus.recordsUpdated = totalRecordsUpdated
  }

  def updateProcessingCacheStatistics(
                                       classificationCacheSize:Int,
                                       locationCacheSize:Int,
                                       storedPointCacheSize:Int,
                                       attributionCacheSize:Int,
                                       spatialLayerCacheSize:Int,
                                       taxonProfileCacheSize:Int,
                                       sensitivityCacheSize:Int,
                                       commonNameCacheSize:Int,
                                       cassandraQueryCacheSize:Int,
                                       timings:Map[String, Long]
                                      ): Unit = {

    processingStatus.classificationCacheSize  = classificationCacheSize
    processingStatus.locationCacheSize = locationCacheSize
    processingStatus.storedPointCacheSize = storedPointCacheSize
    processingStatus.attributionCacheSize = attributionCacheSize
    processingStatus.spatialLayerCacheSize = spatialLayerCacheSize
    processingStatus.taxonProfileCacheSize = taxonProfileCacheSize
    processingStatus.sensitivityCacheSize = sensitivityCacheSize
    processingStatus.commonNameCacheSize = commonNameCacheSize
    processingStatus.cassandraQueryCacheSize = cassandraQueryCacheSize

    //track differences change (increases / decreases)
    processingStatus.defaultProcessorChange = timings.getOrElse("default", 0l) - processingStatus.defaultProcessor
    processingStatus.imageProcessorChange = timings.getOrElse("image", 0l) - processingStatus.imageProcessor
    processingStatus.offlineProcessorChange = timings.getOrElse("offline", 0l) - processingStatus.offlineProcessor
    processingStatus.attributionProcessorChange = timings.getOrElse("attr", 0l) - processingStatus.attributionProcessor
    processingStatus.classificationProcessorChange = timings.getOrElse("class", 0l) - processingStatus.classificationProcessor
    processingStatus.basisOfRecordProcessorChange = timings.getOrElse("bor", 0l) - processingStatus.basisOfRecordProcessor
    processingStatus.eventProcessorChange = timings.getOrElse("event", 0l) - processingStatus.eventProcessor
    processingStatus.locationProcessorChange = timings.getOrElse("loc", 0l) - processingStatus.locationProcessor
    processingStatus.sensitiveProcessorChange = timings.getOrElse("sensitive", 0l) - processingStatus.sensitiveProcessor
    processingStatus.typeStatusProcessorChange = timings.getOrElse("type", 0l) - processingStatus.typeStatusProcessor
    processingStatus.iqProcessorChange = timings.getOrElse("identification", 0l) - processingStatus.iqProcessor
    processingStatus.reProcessorChange = timings.getOrElse("reprocessing", 0l) - processingStatus.reProcessor
    processingStatus.persistChange = timings.getOrElse("persist", 0l) - processingStatus.persist

    //timings
    processingStatus.defaultProcessor = timings.getOrElse("default", 0)
    processingStatus.imageProcessor = timings.getOrElse("image", 0)
    processingStatus.offlineProcessor = timings.getOrElse("offline", 0)
    processingStatus.attributionProcessor = timings.getOrElse("attr", 0)
    processingStatus.classificationProcessor = timings.getOrElse("class", 0)
    processingStatus.basisOfRecordProcessor = timings.getOrElse("bor", 0)
    processingStatus.eventProcessor = timings.getOrElse("event", 0)
    processingStatus.locationProcessor = timings.getOrElse("loc", 0)
    processingStatus.sensitiveProcessor = timings.getOrElse("sensitive", 0)
    processingStatus.typeStatusProcessor = timings.getOrElse("type", 0)
    processingStatus.iqProcessor = timings.getOrElse("identification", 0)
    processingStatus.reProcessor = timings.getOrElse("reprocessing", 0)
    processingStatus.persist= timings.getOrElse("persist", 0l)
  }
}

trait IndexStatusMBean {
  def getTotalRecords:Long
  def getRecordsPerSec : Float
  def getCassandraTime:Long
  def getProcessingTime:Long
  def getSolrTime:Long
  def getTotalTime:Long
  def getIndexDocsCommitted:Long
  def getIndexDocsInRam:Long
  def getIndexDocsInRamMB:Long
  def getProcessingQueue:Long
  def getLuceneQueue:Long
  def getCommitBatchQueue:Long
}

class IndexStatus extends IndexStatusMBean {

  var totalRecords: Long = 0
  var recordsPerSec: Float = 0
  var cassandraTime:Long = 0
  var processingTime:Long = 0
  var solrTime:Long = 0
  var totalTime:Long = 0
  var indexDocsCommitted:Long = 0
  var indexDocsInRam:Long = 0
  var indexDocsInRamMB:Long = 0
  var processingQueue:Long = 0
  var luceneQueue:Long = 0
  var commitBatchQueue:Long = 0

  override def getTotalRecords : Long = totalRecords
  override def getRecordsPerSec : Float = recordsPerSec
  override def getCassandraTime:Long = cassandraTime
  override def getProcessingTime:Long = processingTime
  override def getSolrTime:Long = solrTime
  override def getTotalTime:Long = totalTime
  override def getIndexDocsCommitted:Long = indexDocsCommitted
  override def getIndexDocsInRam:Long = indexDocsInRam
  override def getIndexDocsInRamMB:Long = indexDocsInRamMB
  override def getProcessingQueue:Long = processingQueue
  override def getLuceneQueue:Long = luceneQueue
  override def getCommitBatchQueue:Long = commitBatchQueue
}

trait ProcessingStatusMBean {

  def getRecordsUpdated: Int = 0
  def getRecordsRead: Int = 0
  def getRecordsPerSec: Float = 0
  def getLastPageInSecs : Float = 0

  def getClassificationCacheSize: Int = 0
  def getLocationCacheSize: Int = 0
  def getStoredPointCacheSize: Int = 0
  def getAttributionCacheSize: Int = 0
  def getSpatialLayerCacheSize: Int = 0
  def getTaxonProfileCacheSize: Int = 0
  def getSensitivityCacheSize: Int = 0
  def getCommonNameCacheSize: Int = 0
  def getCassandraQueryCacheSize: Int = 0

  def getDefaultProcessor : Long = 0
  def getImageProcessor : Long = 0
  def getOfflineProcessor : Long = 0
  def getAttributionProcessor : Long = 0
  def getClassificationProcessor : Long = 0
  def getBasisOfRecordProcessor : Long = 0
  def getEventProcessor : Long = 0
  def getLocationProcessor : Long = 0
  def getSensitiveProcessor : Long = 0
  def getTypeStatusProcessor : Long = 0
  def getIqProcessor : Long = 0
  def getReProcessor : Long = 0
  def getPersist : Long = 0

  def getDefaultProcessorChange : Long = 0
  def getImageProcessorChange : Long = 0
  def getOfflineProcessorChange : Long = 0
  def getAttributionProcessorChange : Long = 0
  def getClassificationProcessorChange : Long = 0
  def getBasisOfRecordProcessorChange : Long = 0
  def getEventProcessorChange : Long = 0
  def getLocationProcessorChange : Long = 0
  def getSensitiveProcessorChange : Long = 0
  def getTypeStatusProcessorChange : Long = 0
  def getIqProcessorChange : Long = 0
  def getReProcessorChange : Long = 0
  def getPersistChange : Long = 0

}

class ProcessingStatus extends ProcessingStatusMBean {

  var recordsUpdated: Int = 0
  var recordsRead: Int = 0
  var recordsPerSec: Float = 0
  var lastPageInSecs: Float = 0

  var classificationCacheSize:Int = 0
  var locationCacheSize:Int = 0
  var storedPointCacheSize:Int = 0
  var attributionCacheSize:Int = 0
  var spatialLayerCacheSize:Int = 0
  var taxonProfileCacheSize:Int = 0
  var sensitivityCacheSize:Int = 0
  var commonNameCacheSize:Int = 0
  var cassandraQueryCacheSize:Int = 0

  var defaultProcessor : Long = 0
  var imageProcessor : Long = 0
  var offlineProcessor : Long = 0
  var attributionProcessor : Long = 0
  var classificationProcessor : Long = 0
  var basisOfRecordProcessor : Long = 0
  var eventProcessor : Long = 0
  var locationProcessor : Long = 0
  var sensitiveProcessor : Long = 0
  var typeStatusProcessor : Long = 0
  var iqProcessor : Long = 0
  var reProcessor : Long = 0
  var persist : Long = 0

  var defaultProcessorChange : Long = 0
  var imageProcessorChange : Long = 0
  var offlineProcessorChange : Long = 0
  var attributionProcessorChange : Long = 0
  var classificationProcessorChange : Long = 0
  var basisOfRecordProcessorChange : Long = 0
  var eventProcessorChange : Long = 0
  var locationProcessorChange : Long = 0
  var sensitiveProcessorChange : Long = 0
  var typeStatusProcessorChange : Long = 0
  var iqProcessorChange : Long = 0
  var reProcessorChange : Long = 0
  var persistChange : Long = 0

  override def getRecordsUpdated : Int = recordsUpdated
  override def getRecordsRead : Int = recordsRead
  override def getRecordsPerSec : Float = recordsPerSec
  override def getLastPageInSecs : Float = lastPageInSecs
  override def getClassificationCacheSize:Int  = classificationCacheSize
  override def getLocationCacheSize:Int = locationCacheSize
  override def getStoredPointCacheSize:Int = storedPointCacheSize
  override def getAttributionCacheSize:Int = attributionCacheSize
  override def getSpatialLayerCacheSize:Int = spatialLayerCacheSize
  override def getTaxonProfileCacheSize:Int = taxonProfileCacheSize
  override def getSensitivityCacheSize:Int = sensitivityCacheSize
  override def getCommonNameCacheSize:Int = commonNameCacheSize
  override def getCassandraQueryCacheSize:Int = cassandraQueryCacheSize

  override def getDefaultProcessor : Long = defaultProcessor
  override def getImageProcessor : Long = imageProcessor
  override def getOfflineProcessor : Long = offlineProcessor
  override def getAttributionProcessor : Long = attributionProcessor
  override def getClassificationProcessor : Long = classificationProcessor
  override def getBasisOfRecordProcessor : Long = basisOfRecordProcessor
  override def getEventProcessor : Long = eventProcessor
  override def getLocationProcessor : Long = locationProcessor
  override def getSensitiveProcessor : Long = sensitiveProcessor
  override def getTypeStatusProcessor : Long = typeStatusProcessor
  override def getIqProcessor : Long = iqProcessor
  override def getReProcessor : Long = reProcessor
  override def getPersist: Long = persist

  override def getDefaultProcessorChange : Long = defaultProcessorChange
  override def getImageProcessorChange : Long = imageProcessorChange
  override def getOfflineProcessorChange : Long = offlineProcessorChange
  override def getAttributionProcessorChange : Long = attributionProcessorChange
  override def getClassificationProcessorChange : Long = classificationProcessorChange
  override def getBasisOfRecordProcessorChange : Long = basisOfRecordProcessorChange
  override def getEventProcessorChange : Long = eventProcessorChange
  override def getLocationProcessorChange : Long = locationProcessorChange
  override def getSensitiveProcessorChange : Long = sensitiveProcessorChange
  override def getTypeStatusProcessorChange : Long = typeStatusProcessorChange
  override def getIqProcessorChange : Long = iqProcessorChange
  override def getReProcessorChange : Long = reProcessorChange
  override def getPersistChange : Long = persistChange

}
