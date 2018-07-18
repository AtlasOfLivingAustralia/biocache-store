package au.org.ala.biocache

import au.org.ala.biocache.dao.{QidDAO, QidDAOImpl, OccurrenceDAOImpl, OccurrenceDAO}
import au.org.ala.biocache.index.{SolrIndexDAO, IndexDAO}
import au.org.ala.biocache.persistence.{MockPersistenceManager, PersistenceManager}
import org.scalatest.Ignore
import org.junit.Ignore

@org.junit.Ignore
//The mock config module to be used for the tests
class TestConfigModule extends com.google.inject.AbstractModule {

  override def configure() {
    val properties = {
      val properties = new java.util.Properties()
      properties.load(this.getClass.getResourceAsStream("/biocache-test-config.properties"))
      properties
    }
    com.google.inject.name.Names.bindProperties(this.binder, properties)

    //bind concrete implementations
    bind(classOf[OccurrenceDAO]).to(classOf[OccurrenceDAOImpl]).in(com.google.inject.Scopes.SINGLETON)
    bind(classOf[IndexDAO]).to(classOf[SolrIndexDAO]).in(com.google.inject.Scopes.SINGLETON)
    bind(classOf[QidDAO]).to(classOf[QidDAOImpl]).in(com.google.inject.Scopes.SINGLETON)
    try {
      val nameIndex = new au.org.ala.names.search.ALANameSearcher(properties.getProperty("name.index.dir"))
      bind(classOf[au.org.ala.names.search.ALANameSearcher]).toInstance(nameIndex)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    bind(classOf[PersistenceManager]).to(classOf[MockPersistenceManager]).in(com.google.inject.Scopes.SINGLETON)
  }
}

@org.junit.Ignore
object TestMocks {

  def main(args: Array[String]) {
    val m = new MockPersistenceManager
    m.put("test-uuid", "occ", "dave", "daveValue", true, false)
    m.put("12.12|12.43", "loc", "ibra", "Australian Alps", true, false)
    println(m.get("test-uuid", "occ", "dave"))
    println(m.get("12.12|12.43", "loc", "dave"))
    println(m.get("12.12|12.43sss", "loc", "dave"))
  }
}