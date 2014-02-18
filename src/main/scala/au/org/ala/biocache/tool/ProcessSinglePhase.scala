package au.org.ala.biocache.tool

import au.org.ala.biocache._
import au.org.ala.biocache.processor.TypeStatusProcessor
import scala.Some
import au.org.ala.biocache
import au.org.ala.biocache.model.{Processed, Versions}

object ProcessSinglePhase {

  def main(args:Array[String]){

    val t = new TypeStatusProcessor
    var counter = 0
    Config.occurrenceDAO.pageOverRawProcessed(recordOption => {

      counter += 1
      if(counter % 1000 ==0) println(counter)

      recordOption match {
        case Some(r) => {
          val (raw, processed) = r
          if(raw.identification.typeStatus != null){
            println(raw.rowKey)
            t.process(raw.rowKey, raw, processed)
            biocache.Config.occurrenceDAO.updateOccurrence(raw.rowKey, processed.identification, Processed)
          }
          true
        }
        case None => true
      }
    })
    println("finished")
    biocache.Config.persistenceManager.shutdown
  }
}