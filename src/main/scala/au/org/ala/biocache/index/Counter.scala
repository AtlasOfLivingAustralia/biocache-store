package au.org.ala.biocache.index

import org.slf4j.LoggerFactory

trait Counter {

  val logger = LoggerFactory.getLogger("Counter")

  var counter = 0

  def addToCounter(amount: Int) = counter += amount
  def setCounter(amount: Int) = counter = amount

  var startTime = System.currentTimeMillis
  var finishTime = System.currentTimeMillis

  def printOutStatus(threadId: Int, lastKey: String, runnerType: String, totalTime: Long = 0) = {
    var average = ""
    if (totalTime > 0) {
      average = "Average record/s: " + getAverageRecsPerSec(totalTime)
    }
    finishTime = System.currentTimeMillis
    logger.info("[" + runnerType + " Thread " + threadId + "] " + counter + " >> " + average + ", Last key : " + lastKey)
    startTime = System.currentTimeMillis
  }

  def getAverageRecsPerSec(totalTime: Long = 0) = counter / ((System.currentTimeMillis() - totalTime) / 1000f)
}

class DefaultCounter extends Counter {}