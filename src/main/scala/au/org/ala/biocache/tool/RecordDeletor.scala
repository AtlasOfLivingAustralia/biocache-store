package au.org.ala.biocache.tool

import au.org.ala.biocache.Config
import org.slf4j.LoggerFactory

/**
 * A trait for tools providing record deletion activities.
 */
trait RecordDeletor {

  val logger = LoggerFactory.getLogger("RecordDeletor")
  val pm = Config.persistenceManager
  val indexer = Config.indexDAO
  val occurrenceDAO = Config.occurrenceDAO

  def deleteFromPersistent

  def deleteFromIndex

  def close {
//    pm.shutdown
    indexer.shutdown
  }
}
