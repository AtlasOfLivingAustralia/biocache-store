package au.org.ala.biocache.index

import org.slf4j.LoggerFactory

trait Counter {


  val logger = LoggerFactory.getLogger("Counter")

  var counter = 0

  def addToCounter(amount: Int) = counter += amount

  var startTime = System.currentTimeMillis
  var finishTime = System.currentTimeMillis

  def printOutStatus(threadId: Int, lastKey: String, runnerType: String, totalTime: Long = 0) = {
    var average = ""
    if (totalTime > 0) {
      average = "Average record/s: " + (counter / ((System.currentTimeMillis() - totalTime) / 1000f))
    }
    finishTime = System.currentTimeMillis
    logger.info("[" + runnerType + " Thread " + threadId + "] " + counter + " >> " + average + ", Last key : " + lastKey)
    startTime = System.currentTimeMillis
  }
}

class DefaultCounter extends Counter {}