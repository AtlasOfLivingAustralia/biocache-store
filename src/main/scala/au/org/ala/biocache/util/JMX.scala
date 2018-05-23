package au.org.ala.biocache.util

import java.lang.management.ManagementFactory
import javax.management.ObjectName

object JMX {

  val mbs = ManagementFactory.getPlatformMBeanServer()

  //indexing status
  val indexStatus = new IndexStatus()
  mbs.registerMBean(new IndexStatus(), new ObjectName("au.org.ala.biocache:type=Indexing"))

  //processing status
  val processingStatus = new ProcessingStatus()
  mbs.registerMBean( processingStatus, new ObjectName("au.org.ala.biocache:type=Processing"))

  def updateIndexStatus(averageRecordsPerSec: Float): Unit ={
    indexStatus.averageRecordsPerSec = averageRecordsPerSec
  }

  def updateProcessingStatus(averageRecordsPerSec: Float): Unit ={
    processingStatus.averageRecordsPerSec = averageRecordsPerSec
  }
}

trait IndexStatusMBean {
  def getAverageRecordsPerSec : Float
}

class IndexStatus extends IndexStatusMBean {
  var averageRecordsPerSec: Float = 0
  override def getAverageRecordsPerSec : Float = averageRecordsPerSec
}

trait ProcessingStatusMBean {
  def getAverageRecordsPerSec : Float
}

class ProcessingStatus extends IndexStatusMBean {
  var averageRecordsPerSec: Float = 0
  override def getAverageRecordsPerSec : Float = averageRecordsPerSec
}
