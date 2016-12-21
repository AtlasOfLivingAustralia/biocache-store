package au.org.ala.biocache

import org.scalatest.FunSuite
import com.google.inject.Guice
import au.org.ala.biocache.caches.{SpatialLayerDAO, LocationDAO, TaxonProfileDAO}
import au.org.ala.biocache.model.{TaxonProfile, ConservationStatus}
import au.org.ala.biocache.persistence.PersistenceManager
import org.slf4j.LoggerFactory

/**
 * All tests should extend this to access the correct DI
 */
class ConfigFunSuite extends FunSuite {

  val logger = LoggerFactory.getLogger("ConfigFunSuite")
  System.setProperty("biocache.config","/biocache-test-config.properties")

  Config.inj = Guice.createInjector(new TestConfigModule)
  val pm = Config.persistenceManager
  println("Loading up test suite with persistence manager - " + pm.getClass.getName)
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
//FIXME these will use LocationDAO....
//  //Add some location values - location lookup tests
//  SpatialLayerDAO.add( 144.81060, -35.21667, Map(
//    Config.stateProvinceLayerID -> "New South Wales"
//  ))
//  SpatialLayerDAO.add( 144.8106, -35.21667, Map(
//    Config.stateProvinceLayerID -> "New South Wales"
//  ))
//  SpatialLayerDAO.add( 144.8, -35.2,Map(
//    Config.stateProvinceLayerID -> "New South Wales"
//  ))
//  SpatialLayerDAO.add( 145.52, -40.857, Map(
//    Config.marineLayerID -> "onwater"
//  ))
//  SpatialLayerDAO.add( 133.85720, -23.73750, Map(
//    Config.terrestrialLayerID -> "onland"
//  ))
//  SpatialLayerDAO.add( 146.921099, -31.2532183, Map(
//    Config.stateProvinceLayerID -> "New South Wales"
//  ))
//  SpatialLayerDAO.add( 146.9211, -31.253218, Map(
//    Config.stateProvinceLayerID -> "New South Wales"
//  ))
//  SpatialLayerDAO.add( 167.95, -29.04, Map(
//    Config.countriesLayerID -> "Norfolk Island"
//  ))
//  SpatialLayerDAO.add(144.81060, -35.21667, Map(
//    Config.stateProvinceLayerID -> "New South Wales"
//  ))
//  SpatialLayerDAO.add( 116.5, -31.9, Map(
//    Config.stateProvinceLayerID -> "Western Australia",
//    Config.countriesLayerID -> "Australia"
//  ))
//  SpatialLayerDAO.add( 152.28, -27.56, Map(
//    Config.stateProvinceLayerID -> "Western Australia",
//    Config.countriesLayerID -> "Australia"
//  ))
//  SpatialLayerDAO.add( 116.5122, -31.92223, Map(
//    Config.stateProvinceLayerID -> "Western Australia",
//    Config.countriesLayerID -> "Australia"
//  ))
}