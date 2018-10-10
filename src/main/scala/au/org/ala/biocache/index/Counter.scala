package au.org.ala.biocache.index

import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong

trait Counter {

  val logger = LoggerFactory.getLogger("Counter")

  val counter = new AtomicLong(0)

  def addToCounter(amount: Int) = counter.addAndGet(amount)
  def setCounter(amount: Int) = counter.set(amount)

  val startTime = new AtomicLong(System.currentTimeMillis)
  val finishTime = new AtomicLong(0)

  def printOutStatus(threadId: Int, lastKey: String, runnerType: String, totalTime: Long = 0) = {
    var overallAverage = ""
    finishTime.set(System.currentTimeMillis)
    if (totalTime > 0) {
      overallAverage = "Average record/s: " + getAverageRecsPerSec(totalTime)
    }
    var recentAverage = ""
    if (totalTime > 0) {
      recentAverage = "Average record/s: " + getAverageRecsPerSec(startTime.get())
    }
    startTime.set(System.currentTimeMillis)
    if (logger.isInfoEnabled()) {
      logger.info("[" + runnerType + " Thread " + threadId + "] " + counter.get + " >> overallAverage >> " + overallAverage + " recentAverage >> " + recentAverage + " , Last key : " + lastKey)
    }
  }

  def getAverageRecsPerSec(startTime: Long = 0) = {
    counter.get / ((finishTime.get() - startTime) / 1000f)
  }
}

class DefaultCounter extends Counter {}