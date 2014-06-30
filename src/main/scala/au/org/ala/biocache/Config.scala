package au.org.ala.biocache

import org.slf4j.LoggerFactory
import com.google.inject.{Scopes, AbstractModule, Guice, Injector}
import au.org.ala.names.search.ALANameSearcher
import au.org.ala.sds.SensitiveSpeciesFinderFactory
import org.ala.layers.client.Client
import java.util.Properties
import java.io.FileInputStream
import com.google.inject.name.Names
import au.org.ala.biocache.dao._
import au.org.ala.biocache.index.{SolrIndexDAO, IndexDAO}
import au.org.ala.biocache.persistence.{PostgresPersistenceManager, PersistenceManager, CassandraPersistenceManager}
import au.org.ala.biocache.vocab.StateProvinces
import au.org.ala.biocache.load.{RemoteMediaStore, LocalMediaStore}

/**
 * Simple singleton wrapper for Guice that reads from a properties file
 * and initialises the biocache configuration including database connections
 * and search indexes.
 */
object Config {

  protected val logger = LoggerFactory.getLogger("Config")
  private val configModule = new ConfigModule()
  var inj:Injector = Guice.createInjector(configModule)
  def getInstance(classs:Class[_]) = inj.getInstance(classs)

  //persistence
  val persistenceManager = getInstance(classOf[PersistenceManager]).asInstanceOf[PersistenceManager]

  //daos
  val occurrenceDAO = getInstance(classOf[OccurrenceDAO]).asInstanceOf[OccurrenceDAO]
  val outlierStatsDAO = getInstance(classOf[OutlierStatsDAO]).asInstanceOf[OutlierStatsDAO]
  val deletedRecordDAO = getInstance(classOf[DeletedRecordDAO]).asInstanceOf[DeletedRecordDAO]
  val duplicateDAO = getInstance(classOf[DuplicateDAO]).asInstanceOf[DuplicateDAO]
  val validationRuleDAO = getInstance(classOf[ValidationRuleDAO]).asInstanceOf[ValidationRuleDAO]
  val indexDAO = getInstance(classOf[IndexDAO]).asInstanceOf[IndexDAO]

  val mediaStore = {
    val str = configModule.properties.getProperty("media.store.local", "true")
    if(str.toBoolean){
      logger.info("Using local media store")
      LocalMediaStore
    } else {
      logger.info("Using remote media store")
      RemoteMediaStore
    }
  }

  val remoteMediaStoreUrl = configModule.properties.getProperty("media.store.url", "http://10.1.1.2/images")

  //name index
  val nameIndex = getInstance(classOf[ALANameSearcher]).asInstanceOf[ALANameSearcher]

  //load sensitive data service
  lazy val sdsFinder = {
    val sdsUrl = configModule.properties.getProperty("sds.url","http://sds.ala.org.au/sensitive-species-data.xml")
    SensitiveSpeciesFinderFactory.getSensitiveSpeciesFinder(sdsUrl, nameIndex)
  }

  val allowLayerLookup = {
    val str = configModule.properties.getProperty("allow.layer.lookup")
    if(str != null){
      str.toBoolean
    } else {
      false
    }
  }

  val volunteerHubUid = configModule.properties.getProperty("volunteer.hub.uid","")

  val collectoryApiKey = configModule.properties.getProperty("registry.api.key","xxxxxxxxxxxxxxxxx")

  val loadFileStore = configModule.properties.getProperty("load.dir","/data/biocache-load/")

  val deletedFileStore = configModule.properties.getProperty("deleted.file.store","/data/biocache-delete/")

  val mediaFileStore = configModule.properties.getProperty("media.dir","/data/biocache-media/")

  val mediaBaseUrl = configModule.properties.getProperty("media.url","http://biocache.ala.org.au/biocache-media")

  val excludeSensitiveValuesFor = configModule.properties.getProperty("exclude.sensitive.values","")

  val allowCollectoryUpdates = configModule.properties.getProperty("allow.registry.updates","false")

  val extraMiscFields = configModule.properties.getProperty("extra.misc.fields","")

  val technicalContact = configModule.properties.getProperty("technical.contact", "support@ala.org.au")

  lazy val fieldsToSample = {

    val str = configModule.properties.getProperty("sample.fields")

    val defaultFields = configModule.properties.getProperty("default.sample.fields", "")

    if (str == null || str.trim == "" ){
      val dbfields = try {
        Client.getLayerIntersectDao.getConfig.getFieldsByDB
      } catch {
        case e:Exception => new java.util.ArrayList()
      }

      val fields: Array[String] = if(dbfields.size > 0){
        Array.ofDim(dbfields.size())
      } else {
        defaultFields.split(",").map(x => x.trim).toArray
      }

      if(dbfields.size > 0){
          for (a <- 0 until dbfields.size()) {
            fields(a) = dbfields.get(a).getId()
          }
      }
      logger.info("Fields to sample: " + fields.mkString(","))
      fields   //fields.dropWhile(x => List("el898","cl909","cl900").contains(x))
    } else if (str == "none"){
      Array[String]()
    } else {
      val fields = str.split(",").map(x => x.trim).toArray
      logger.info("Fields to sample: " + fields.mkString(","))
      fields
    }
  }

  val speciesSubgroupsUrl = configModule.properties.getProperty("species.subgroups.url","http://bie.ala.org.au/subgroups.json")

  val listToolUrl = configModule.properties.getProperty("list.tool.url","http://lists.ala.org.au/ws")

  val volunteerUrl = configModule.properties.getProperty("volunteer.url","http://volunteer.ala.org.au")

  val tmpWorkDir = configModule.properties.getProperty("tmp.work.dir","/tmp")

  val registryUrl = configModule.properties.getProperty("registry.url","http://collections.ala.org.au/ws")

  lazy val flickrUsersUrl = configModule.properties.getProperty("flickr.users.url", "http://auth.ala.org.au/userdetails/external/flickr")

  lazy val reindexUrl = configModule.properties.getProperty("reindex.url")

  lazy val reindexData = configModule.properties.getProperty("reindex.data")

  lazy val reindexViewDataResourceUrl = configModule.properties.getProperty("reindex.data.resource.url")

  lazy val layersServiceUrl = configModule.properties.getProperty("layers.service.url")

  lazy val biocacheServiceUrl = configModule.properties.getProperty("webservices.root","http://biocache.ala.org.au/ws")

  def getProperty(prop:String) = configModule.properties.getProperty(prop)

  def getProperty(prop:String, default:String) = configModule.properties.getProperty(prop,default)

  def outputConfig = configModule.properties.list(System.out)

  //layer defaults
  val stateProvinceLayerID = configModule.properties.getProperty("layer.state.province", "cl927")
  val terrestrialBioRegionsLayerID = configModule.properties.getProperty("layer.bio.regions", "cl20")
  val marineBioRegionsLayerID = configModule.properties.getProperty("layer.bio.regions", "cl21")
  val countriesLayerID = configModule.properties.getProperty("layer.countries", "cl932")
  val localGovLayerID = configModule.properties.getProperty("layer.countries", "cl23")

  //used by location processor for associating a country with an occurrence record where only stateProvince supplied
  val defaultCountry = configModule.properties.getProperty("default.country", "Australia")
}

/**
 * Guice configuration module.
 */
private class ConfigModule extends AbstractModule {

  protected val logger = LoggerFactory.getLogger("ConfigModule")

  val properties = {

    val properties = new Properties()
    //NC 2013-08-16: Supply the properties file as a system property via -Dbiocache.config=<file>
    //or the default /data/biocache/config/biocache-config.properties file is used.

    //check to see if a system property has been supplied with the location of the config file
    val filename = System.getProperty("biocache.config","/data/biocache/config/biocache-config.properties")
    val file = new java.io.File(filename)

    //only load the properties file if it exists otherwise default to the biocache.properties on the classpath
    val stream = if(file.exists()) {
      new FileInputStream(file)
    } else {
      this.getClass.getResourceAsStream("/biocache.properties")
    }

    if(stream == null){
      throw new RuntimeException("Configuration file not found. Please add to classpath or /data/biocache/config/biocache-config.properties")
    }

    logger.debug("Loading configuration from " + filename)
    properties.load(stream)

    properties
  }

  override def configure() {

    Names.bindProperties(this.binder, properties)
    //bind concrete implementations
    bind(classOf[OccurrenceDAO]).to(classOf[OccurrenceDAOImpl]).in(Scopes.SINGLETON)
    bind(classOf[OutlierStatsDAO]).to(classOf[OutlierStatsDAOImpl]).in(Scopes.SINGLETON)
    logger.debug("Initialising SOLR")
    bind(classOf[IndexDAO]).to(classOf[SolrIndexDAO]).in(Scopes.SINGLETON)
    bind(classOf[DeletedRecordDAO]).to(classOf[DeletedRecordDAOImpl]).in(Scopes.SINGLETON)
    bind(classOf[DuplicateDAO]).to(classOf[DuplicateDAOImpl]).in(Scopes.SINGLETON)
    bind(classOf[ValidationRuleDAO]).to(classOf[ValidationRuleDAOImpl]).in(Scopes.SINGLETON)
    logger.debug("Initialising name matching indexes")
    try {
      val nameIndexLocation = properties.getProperty("name.index.dir")
      logger.debug("Loading name index from " + nameIndexLocation)
      val nameIndex = new ALANameSearcher(nameIndexLocation)
      bind(classOf[ALANameSearcher]).toInstance(nameIndex)
    } catch {
      case e: Exception => logger.warn("Lucene indexes are not currently available. Please check 'name.index.dir' property in config. Message: " + e.getMessage())
    }
    logger.debug("Initialising persistence manager")
    properties.getProperty("db") match {
      //case "mock" => bind(classOf[PersistenceManager]).to(classOf[MockPersistenceManager]).in(Scopes.SINGLETON)
      case "postgres" => bind(classOf[PersistenceManager]).to(classOf[PostgresPersistenceManager]).in(Scopes.SINGLETON)
      //case "cassandra" => bind(classOf[PersistenceManager]).to(classOf[CassandraPersistenceManager]).in(Scopes.SINGLETON)
      //case "mongodb" => bind(classOf[PersistenceManager]).to(classOf[MongoDBPersistenceManager]).in(Scopes.SINGLETON)
      case _ => bind(classOf[PersistenceManager]).to(classOf[CassandraPersistenceManager]).in(Scopes.SINGLETON)
    }
    logger.debug("Configure complete")
  }
}