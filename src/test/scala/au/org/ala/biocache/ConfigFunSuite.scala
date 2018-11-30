package au.org.ala.biocache

import java.io.File

import org.scalatest.FunSuite
import com.google.inject.Guice
import au.org.ala.biocache.caches.{LocationDAO, SpatialLayerDAO, TaxonProfileDAO}
import au.org.ala.biocache.model.{ConservationStatus, TaxonProfile}
import au.org.ala.biocache.persistence.PersistenceManager
import org.slf4j.LoggerFactory

/**
 * All tests should extend this to access the correct DI
 */
class ConfigFunSuite extends FunSuite {

  val logger = LoggerFactory.getLogger("ConfigFunSuite")

  val configFilePath = System.getProperty("user.dir") +  "/src/test/resources/biocache-test-config.properties"

  System.setProperty("biocache.config", configFilePath)
//  println("Using test config " + configFilePath)
//  println("Using test config exists : " + new File(configFilePath).exists())

  Config.inj = Guice.createInjector(new TestConfigModule)
  val pm = Config.persistenceManager
  //Web services will automatically grab the location details that are necessary
  //Add conservation status
  val taxonProfile = new TaxonProfile
  taxonProfile.setGuid("urn:lsid:biodiversity.org.au:afd.taxon:3809b1ca-8b60-4fcb-acf5-ca4f1dc0e263")
  taxonProfile.setConservation(Array(
    new ConservationStatus("Victoria", "aus_states/Victoria", "Endangered", "Endangered"),
    new ConservationStatus("Victoria", "aus_states/Victoria", null, "Listed under FFG Act")
  ))
  TaxonProfileDAO.add(taxonProfile)

  val tp = new TaxonProfile
  tp.setGuid("urn:lsid:biodiversity.org.au:afd.taxon:aa745ff0-c776-4d0e-851d-369ba0e6f537")
  tp.setHabitats(Array("Terrestrial"))
  TaxonProfileDAO.add(tp)
}