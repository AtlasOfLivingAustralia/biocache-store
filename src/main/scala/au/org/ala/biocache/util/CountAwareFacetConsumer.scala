package au.org.ala.biocache.util

import au.org.ala.biocache.tool.DuplicationDetection
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.BlockingQueue
import org.slf4j.LoggerFactory

class CountAwareFacetConsumer(q: BlockingQueue[String], id: Int, sentinel: String, proc: Array[String] => Unit, countSize: Int = 0, minSize: Int = 1) extends Thread {
  val logger = LoggerFactory.getLogger("CountAwareFacetConsumer")

  override def run() {
    val buf = new ArrayBuffer[String]()
    var counter = 0
    var batchSize = 0
    var finished = false
    try {
      while (!finished && !Thread.currentThread().isInterrupted()) {
        try {
          //wait 1 second before assuming that the queue is empty
          val value = q.poll(1, java.util.concurrent.TimeUnit.SECONDS)
          if (value == sentinel) {
            if (logger.isDebugEnabled()) {
              logger.debug("CountAwareFacetConsumer " + id + " is returning on sentinel")
            }
            finished = true
          } else if (value != null) {
            if (logger.isDebugEnabled()) {
              logger.debug("Count Aware Consumer " + id + " is handling " + value)
            }
            val values = value.split("\t")
            val count = Integer.parseInt(values(1))
            if (count >= minSize) {
              counter += count
              batchSize += 1
              buf += values(0)
              if (counter >= countSize || batchSize == 200) {
                val array = buf.toArray
                buf.clear()
                counter = 0
                batchSize = 0
                proc(array)
              }
            }
          }
        } catch {
          case interrupted: InterruptedException => throw interrupted
          case e: Exception => e.printStackTrace()
        }
      }
    } finally {
      try {
        // process anything that was left in the buffer after the queue was emptied
        val array = buf.toArray
        buf.clear()
        if (!array.isEmpty) {
          proc(array)
        }
      } catch {
        case interrupted: InterruptedException => throw interrupted
        case e: Exception => e.printStackTrace()
      }
    }
    logger.debug("Stopping " + id)
  }
}