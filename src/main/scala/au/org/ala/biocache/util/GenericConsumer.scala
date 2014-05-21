package au.org.ala.biocache.util

import au.org.ala.biocache.tool.DuplicationDetection
import java.util.concurrent.BlockingQueue
import org.slf4j.LoggerFactory

class GenericConsumer[T](q:BlockingQueue[T], id:Int, proc: (T,Int) =>Unit) extends Thread {

  val logger = LoggerFactory.getLogger("GenericConsumer")

  var shouldStop = false

  override def run(){
    while(!shouldStop || q.size()>0){
      try{
        //wait 1 second before assuming that the queue is empty
        val value = q.poll(1,java.util.concurrent.TimeUnit.SECONDS)
        if(value !=null){
          DuplicationDetection.logger.debug("Generic Consumer " + id + " is handling " + value)
          proc(value, id)
        }
      }
      catch{
        case e:Exception=> e.printStackTrace()
      }
    }
    logger.debug("Stopping " + id)
  }
}