package au.org.ala.biocache

import au.org.ala.biocache.caches.{SpatialLayerDAO, TaxonProfileDAO}
import au.org.ala.biocache.model.{ConservationStatus, TaxonProfile}
import com.google.inject.Guice
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

/**
 * All tests should extend this to access the correct DI
 */
class ScottishConfigFunSuite extends FunSuite {
  val logger = LoggerFactory.getLogger("ScottishConfigFunSuite")
  System.setProperty("biocache.config","/biocache-als-config.properties")
  Config.inj = Guice.createInjector(new TestConfigModule)
  val pm = Config.persistenceManager
}