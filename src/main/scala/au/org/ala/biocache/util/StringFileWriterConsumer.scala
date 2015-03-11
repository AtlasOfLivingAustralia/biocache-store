package au.org.ala.biocache.util

import java.io.FileWriter
import java.util.concurrent.BlockingQueue

import org.slf4j.LoggerFactory

/**
 * A generic threaded consumer that takes a string and FileWriter and calls the supplied proc
 */
class StringFileWriterConsumer(q: BlockingQueue[String], id: Int, csvFileWriter: FileWriter, csvFileWriterSensitive: FileWriter, proc: (String, FileWriter, FileWriter) => Unit) extends Thread {

  protected val logger = LoggerFactory.getLogger("StringFileWriterConsumer")

  var shouldStop = false

  override def run() {
    while (!shouldStop || q.size() > 0) {
      try {
        //wait 1 second before assuming that the queue is empty
        val guid = q.poll(1, java.util.concurrent.TimeUnit.SECONDS)
        if (guid != null) {
          logger.debug("Guid Consumer " + id + " is handling " + guid)
          proc(guid, csvFileWriter, csvFileWriterSensitive)
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    logger.debug("Stopping " + id)
  }
}
