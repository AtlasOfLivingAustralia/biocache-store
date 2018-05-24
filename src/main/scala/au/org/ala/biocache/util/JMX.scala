package au.org.ala.biocache.util

import java.lang.management.ManagementFactory
import javax.management.ObjectName

import au.org.ala.biocache.caches._

object JMX {

  val mbs = ManagementFactory.getPlatformMBeanServer()

  //indexing status
  val indexStatus = new IndexStatus()
  mbs.registerMBean(new IndexStatus(), new ObjectName("au.org.ala.biocache:type=Indexing"))

  //processing status
  val processingStatus = new ProcessingStatus()
  mbs.registerMBean( processingStatus, new ObjectName("au.org.ala.biocache:type=Processing"))

//  centralCounter.getAverageRecsPerSec(startTimeFinal), //records per sec
//  t2Total.get() / 1000000000, //cassandra time
//  timing.get() / 1000000000, //processing time
//  luceneIndexing(0).getTiming / 1000000000, //solr time
//  (System.currentTimeMillis - timeCounter.get) / 1000, // totalTime
//  luceneIndexing(0).getCount, //index docs committed
//  luceneIndexing(0).ramDocs(), //index docs in ram
//  (luceneIndexing(0).ramBytes() / 1024 / 1024), //index docs ram MB
//  queue.size(), //processing queue
//  luceneIndexing(0).getQueueSize, //lucene queue
//  luceneIndexing(0).getBatchSize //commit batch
//


  def updateIndexStatus(recordsPerSec: Float): Unit ={
    indexStatus.recordsPerSec = recordsPerSec
  }

  def updateProcessingStatus(recordsPerSec: Float): Unit ={
    processingStatus.recordsPerSec = recordsPerSec
  }

  def updateProcessingStats(
                             recordsPerSec: Float
                           ): Unit ={
    processingStatus.recordsPerSec = recordsPerSec
  }

  def  updateProcessingCacheStatistics(
                                       classificationCacheSize:Int,
                                       locationCacheSize:Int,
                                       storedPointCacheSize:Int,
                                       attributionCacheSize:Int,
                                       spatialLayerCacheSize:Int,
                                       taxonProfileCacheSize:Int,
                                       sensitivityCacheSize:Int): Unit = {
    processingStatus.classificationCacheSize  = classificationCacheSize
    processingStatus.locationCacheSize = locationCacheSize
    processingStatus.storedPointCacheSize = storedPointCacheSize
    processingStatus.attributionCacheSize = attributionCacheSize
    processingStatus.spatialLayerCacheSize = spatialLayerCacheSize
    processingStatus.taxonProfileCacheSize = taxonProfileCacheSize
    processingStatus.sensitivityCacheSize = sensitivityCacheSize
  }
}

trait IndexStatusMBean {
  def getRecordsPerSec : Float
}

class IndexStatus extends IndexStatusMBean {
  var recordsPerSec: Float = 0
  override def getRecordsPerSec : Float = recordsPerSec
}

trait ProcessingStatusMBean {
  def getRecordsPerSec: Float = 0

  def getClassificationCacheSize: Int = 0

  def getLocationCacheSize: Int = 0

  def getStoredPointCacheSize: Int = 0

  def getAttributionCacheSize: Int = 0

  def getSpatialLayerCacheSize: Int = 0

  def getTaxonProfileCacheSize: Int = 0

  def getSensitivityCacheSize: Int = 0
}

class ProcessingStatus extends ProcessingStatusMBean {

  var recordsPerSec: Float = 0
  var classificationCacheSize:Int = 0
  var locationCacheSize:Int = 0
  var storedPointCacheSize:Int = 0
  var attributionCacheSize:Int = 0
  var spatialLayerCacheSize:Int = 0
  var taxonProfileCacheSize:Int = 0
  var sensitivityCacheSize:Int = 0

  override def getRecordsPerSec : Float = recordsPerSec
  override def getClassificationCacheSize:Int  = classificationCacheSize
  override def getLocationCacheSize:Int = locationCacheSize
  override def getStoredPointCacheSize:Int = storedPointCacheSize
  override def getAttributionCacheSize:Int = attributionCacheSize
  override def getSpatialLayerCacheSize:Int = spatialLayerCacheSize
  override def getTaxonProfileCacheSize:Int = taxonProfileCacheSize
  override def getSensitivityCacheSize:Int = sensitivityCacheSize
}
