package au.org.ala.biocache.util

import java.util.concurrent.BlockingQueue
import au.org.ala.biocache.tool.DuplicationDetection
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A generic threaded consumer that takes a string and calls the supplied proc
 */
class StringConsumer(q: BlockingQueue[String], id: Int, sentinel: String, proc: String => Unit) extends Thread {

  protected val logger = LoggerFactory.getLogger("StringConsumer")

  override def run() {
    if (logger.isDebugEnabled()) {
      logger.debug("Starting StringConsumer " + id)
    }
    var finished = false
    while (!finished && !Thread.currentThread().isInterrupted()) {
      try {
        val guid = q.poll(1, java.util.concurrent.TimeUnit.SECONDS)
        if (guid == sentinel) {
          if (logger.isDebugEnabled()) {
            logger.debug("StringConsumer " + id + " is returning on sentinel")
          }
          finished = true
        } else if (guid != null) {
          if (logger.isDebugEnabled()) {
            logger.debug("StringConsumer " + id + " is handling " + guid)
          }
          proc(guid)
        }
      } catch {
        case interrupted: InterruptedException => {
          if (logger.isDebugEnabled()) {
            logger.debug("Interrupted StringConsumer " + id)
          }
          Thread.currentThread().interrupt()
          finished = true
          throw interrupted
        }
        case e: Exception => e.printStackTrace()
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Stopping StringConsumer " + id)
    }
  }
}
