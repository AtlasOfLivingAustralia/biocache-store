package au.org.ala.biocache

import java.io.{File, FileInputStream}
import java.util.Properties
import java.util.jar.Attributes

import au.org.ala.biocache.caches.SpatialLayerDAO
import au.org.ala.biocache.dao._
import au.org.ala.biocache.index.{IndexDAO, SolrIndexDAO}
import au.org.ala.biocache.load.{LocalMediaStore, NullMediaStore, RemoteMediaStore}
import au.org.ala.biocache.persistence._
import au.org.ala.biocache.util.LayersStore
import au.org.ala.names.search.ALANameSearcher
import au.org.ala.sds.{SensitiveSpeciesFinder, SensitiveSpeciesFinderFactory}
import com.google.inject.name.Names
import com.google.inject.{AbstractModule, Guice, Injector, Scopes}
import org.apache.commons.lang3.{BooleanUtils, StringUtils}
import org.slf4j.LoggerFactory

import scala.io.Source

/**
 * Simple singleton wrapper for Guice that reads from a properties file
 * and initialises the biocache configuration including database connections
 * and search indexes.
 */
object Config {

  import collection.JavaConversions._

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

  //URL to an instance of the ALA image-service
  val remoteMediaStoreUrl = configModule.properties.getProperty("media.store.url", "")

  // Media store type
  val mediaStoreType = configModule.properties.getProperty("media.store.type", "auto")

  // Media not found image URL
  val mediaNotFound = configModule.properties.getProperty("media.store.notFound.url", "https://www.ala.org.au/commonui-bs3/img/icon-camera.png")

  val mediaStore = {
    (mediaStoreType) match {
      case "none" => NullMediaStore
      case "local" => LocalMediaStore
      case "remote" => RemoteMediaStore
      case _ => if (StringUtils.isBlank(remoteMediaStoreUrl)) {
        logger.debug("Using local media store")
        LocalMediaStore
      } else {
        logger.debug("Using remote media store")
        RemoteMediaStore
      }
    }
  }

  //name index
  val nameIndex = getInstance(classOf[ALANameSearcher]).asInstanceOf[ALANameSearcher]

  val hashImageFileNames = configModule.properties.getProperty("hash.image.filenames", "false").toBoolean

  val solrHome = configModule.properties.getProperty("solr.home", "")

  val solrConnectionPoolSize = configModule.properties.getProperty("solr.connection.pool.size", "50").toInt

  // Set to the same as the connection pool size by default, assuming a single Solr server
  // Configure this and the connection pool size as required to fit a Solr cluster setup if one is in use
  val solrConnectionMaxPerRoute = configModule.properties.getProperty("solr.connection.pool.maxperroute", "50").toInt

  val solrConnectionConnectTimeout = configModule.properties.getProperty("solr.connection.connecttimeout", "30000").toInt

  val solrConnectionRequestTimeout = configModule.properties.getProperty("solr.connection.requesttimeout", "30000").toInt

  val solrConnectionSocketTimeout = configModule.properties.getProperty("solr.connection.sockettimeout", "30000").toInt

  val solrConnectionCacheEntries = configModule.properties.getProperty("solr.connection.cache.entries", "500").toInt

  // 1024 * 256 = 262144 bytes
  val solrConnectionCacheObjectSize = configModule.properties.getProperty("solr.connection.cache.object.size", "262144").toInt

  val userAgent = configModule.properties.getProperty("biocache.useragent", "Biocache")

  val solrUpdateThreads = configModule.properties.getProperty("solr.update.threads", "4").toInt

  val cassandraUpdateThreads = configModule.properties.getProperty("cassandra.update.threads", "8").toInt

  val cassandraFetchSize = configModule.properties.getProperty("cassandra.fetch.size", "500").toInt

  val cassandraTimeout = configModule.properties.getProperty("cassandra.timeout", "120000").toInt

  val volunteerHubUid = configModule.properties.getProperty("volunteer.hub.uid","")

  val volunteerDataProviderUid = configModule.properties.getProperty("volunteer.dp.uid", "")

  val collectoryApiKey = configModule.properties.getProperty("registry.api.key","xxxxxxxxxxxxxxxxx")

  val loadFileStore = configModule.properties.getProperty("load.dir","/data/biocache-load/")

  val vocabDirectory = configModule.properties.getProperty("vocab.dir","/data/biocache/vocab/")

  val layersDirectory = configModule.properties.getProperty("layers.dir","/data/biocache/layers/")

  val deletedFileStore = configModule.properties.getProperty("deleted.file.store","/data/biocache-delete/")

  val outlierLayerIDs = configModule.properties.getProperty("outlier.layers","el882,el889,el887,el865,el894").split(",").map {_.trim}

  val mediaFileStore = configModule.properties.getProperty("media.dir","/data/biocache-media/")

  val mediaBaseUrl = configModule.properties.getProperty("media.url","https://biocache.ala.org.au/biocache-media")

  val excludeSensitiveValuesFor = configModule.properties.getProperty("exclude.sensitive.values","")

  val solrCollection = configModule.properties.getProperty("solr.collection", "biocache1")

  val allowCollectoryUpdates = configModule.properties.getProperty("allow.registry.updates","false")

  val commonNameLanguages:Array[String] = {
    val configValue = configModule.properties.getProperty("commonname.lang","")
    if(StringUtils.isNotEmpty(configValue)){
      configValue.split(",").map(_.toLowerCase.trim)
    } else {
      Array[String]()
    }
  }

  val extraMiscFields = configModule.properties.getProperty("extra.misc.fields","")

  val technicalContact = configModule.properties.getProperty("technical.contact", "support@ala.org.au")

  val irmngDwcArchiveUrl = configModule.properties.getProperty("irmng.archive.url", "http://www.cmar.csiro.au/datacentre/downloads/IRMNG_DWC.zip")

  /** Whether or not to strictly obey the isLoadable directive from the SDS */
  val obeySDSIsLoadable = configModule.properties.getProperty("obey.sds.is.loadable", "true").toBoolean

  /** a regex pattern for identifying guids associated with the national checklists */
  val nationalChecklistIdentifierPattern = configModule.properties.getProperty("national.checklist.guid.pattern", """biodiversity.org.au""")

  val taxonProfileCacheAll = configModule.properties.getProperty("taxon.profile.cache.all", "false").toBoolean
  val taxonProfileCacheSize = configModule.properties.getProperty("taxon.profile.cache.size", "10000").toInt
  val classificationCacheSize = configModule.properties.getProperty("classification.cache.size", "10000").toInt
  val commonNameCacheSize = configModule.properties.getProperty("commonname.cache.size", "10000").toInt
  val spatialCacheSize = configModule.properties.getProperty("spatial.cache.size", "10000").toInt
  val attributionCacheSize = configModule.properties.getProperty("attribution.cache.size", "10000").toInt
  val sensitivityCacheSize = configModule.properties.getProperty("sensitivity.cache.size", "10000").toInt
  val locationCacheSize = configModule.properties.getProperty("location.cache.size", "10000").toInt
  val dateFormatCacheSize = configModule.properties.getProperty("dateformat.cache.size", "10000").toInt
  val jmxDebugEnabled = configModule.properties.getProperty("jmx.debug.enabled", "true").toBoolean

  /** To index or only store, by default, all new misc fields */
  val solrIndexMisc: Boolean = configModule.properties.getProperty("solr.index.misc", "false").toBoolean

  /** default values for new schema fields */
  val schemaFieldTypeCl: String = configModule.properties.getProperty("solr.index.fieldtype.cl", "string")
  val schemaFieldTypeEl: String = configModule.properties.getProperty("solr.index.fieldtype.el", "tfloat")
  val schemaMultiValuedLayer: Boolean = configModule.properties.getProperty("solr.index.multivalued.layer", "false").toBoolean
  val schemaDocValuesLayer: Boolean = configModule.properties.getProperty("solr.index.docvalues.layer", "false").toBoolean
  val schemaIndexedLayer: Boolean = configModule.properties.getProperty("solr.index.indexed.layer", "true").toBoolean
  val schemaStoredLayer: Boolean = configModule.properties.getProperty("solr.index.stored.layer", "true").toBoolean
  val schemaFieldTypeMisc: String = configModule.properties.getProperty("solr.index.fieldtype.misc", "string")
  val schemaMultiValuedMisc: Boolean = configModule.properties.getProperty("solr.index.multivalued.misc", "false").toBoolean
  val schemaDocValuesMisc: Boolean = configModule.properties.getProperty("solr.index.docvalues.misc", "false").toBoolean
  val schemaStoredMisc: Boolean = configModule.properties.getProperty("solr.index.stored.misc", "true").toBoolean

  private var fieldsToSampleCached = Array[String]()

  def fieldsToSample(refresh:Boolean = true) = {
    if (refresh || fieldsToSampleCached.isEmpty) {
      val str = configModule.properties.getProperty("sample.fields")

      val defaultFields = configModule.properties.getProperty("default.sample.fields", "")

      if (str == null || str.trim == "" || str.trim == "all") {
        val dbfields = try {
          new LayersStore(Config.layersServiceUrl).getFieldIds()
        } catch {
          case e: Exception => {
            logger.error("Problem loading layers to intersect: " + e.getMessage, e)
            new java.util.ArrayList()
          }
        }

        logger.info("Number of fields to sample: " + dbfields.size())

        val fields: Array[String] = if (!dbfields.isEmpty) {
          Array.ofDim(dbfields.size())
        } else {
          defaultFields.split(",").map(x => x.trim).toArray
        }

        if (!dbfields.isEmpty) {
          for (a <- 0 until dbfields.size()) {
            fields(a) = dbfields.get(a)
          }
        }
        logger.info("Fields to sample: " + fields.mkString(","))
        fieldsToSampleCached = fields
      } else if (str == "none") {
        fieldsToSampleCached = Array[String]()
      } else {
        val fields = str.split(",").map(x => x.trim).toArray
        logger.info("Fields to sample: " + fields.mkString(","))
        fieldsToSampleCached = fields
      }
    }
    fieldsToSampleCached
  }

  val blacklistedMediaUrls = {
    val blacklistMediaUrlsFile = configModule.properties.getProperty("blacklist.media.file","/data/biocache/config/blacklistMediaUrls.txt")
    if(new File(blacklistMediaUrlsFile).exists()){
      logger.info("Using the set of blacklisted media URLs defined in: " + blacklistMediaUrlsFile)
      Source.fromFile(new File(blacklistMediaUrlsFile)).getLines().map(_.trim()).toList
    } else {
      logger.info("Using the default set of blacklisted media URLs")
      List("http://www.inaturalist.org/photos/", "http://www.flickr.com/photos/", "http://www.facebook.com/photo.php", "https://picasaweb.google.com")
    }
  }


  val speciesGroupsUrl = configModule.properties.getProperty("species.groups.url","")

  val speciesSubgroupsUrl = configModule.properties.getProperty("species.subgroups.url","https://bie.ala.org.au/subgroups.json")

  val listToolUrl = configModule.properties.getProperty("list.tool.url","https://lists.ala.org.au")

  val volunteerUrl = configModule.properties.getProperty("volunteer.url","https://volunteer.ala.org.au")

  val tmpWorkDir = configModule.properties.getProperty("tmp.work.dir","/tmp")

  val registryUrl = configModule.properties.getProperty("registry.url","https://collections.ala.org.au/ws")

  val persistPointsFile = configModule.properties.getProperty("persist.points.file", "")

  lazy val flickrUsersUrl = configModule.properties.getProperty("flickr.users.url", "https://auth.ala.org.au/userdetails/external/flickr")

  lazy val reindexUrl = configModule.properties.getProperty("reindex.url")

  lazy val reindexData = configModule.properties.getProperty("reindex.data")

  lazy val reindexViewDataResourceUrl = configModule.properties.getProperty("reindex.data.resource.url")

  lazy val layersServiceUrl = configModule.properties.getProperty("layers.service.url")

  lazy val layersServiceSampling = configModule.properties.getProperty("layers.service.sampling", "true").toBoolean

  lazy val layerServiceRetries = configModule.properties.getProperty("layers.service.retries", "10").toInt

  lazy val layerServiceRetryWait = configModule.properties.getProperty("layers.service.retry.wait", "30000").toInt

  lazy val biocacheServiceUrl = configModule.properties.getProperty("webservices.root","https://biocache-ws.ala.org.au/ws")

  lazy val solrBatchSize = configModule.properties.getProperty("solr.batch.size", "1000").toInt

  lazy val solrHardCommitSize = configModule.properties.getProperty("solr.hardcommit.size", "10000").toInt

  val stateProvincePrefixFields = configModule.properties.getProperty("species.list.prefix","stateProvince").split(",").toSet

  val speciesListIndexValues = configModule.properties.getProperty("species.list.index.keys", "category,status,sourceStatus").split(",").toSet

  val loadSpeciesLists = configModule.properties.getProperty("include.species.lists", "false").toBoolean

  val taxonProfilesEnabled = configModule.properties.getProperty("taxon.profiles.enabled", "true").toBoolean

  val localNodeIp = configModule.properties.getProperty("local.node.ip", "127.0.0.1")

  val zookeeperAddress = configModule.properties.getProperty("zookeeper.address", "127.0.0.1:2181")

  val zookeeperUpdatesEnabled = configModule.properties.getProperty("zookeeper.updates.enabled", "false").toBoolean

  /** The node number. Not these are indexed from 0  */
  val nodeNumber = configModule.properties.getProperty("node.number", "0").toInt

  val cassandraTokenSplit = configModule.properties.getProperty("cassandra.token.split", "1").toInt

  val clusterSize = configModule.properties.getProperty("cluster.size", "1").toInt

  def getProperty(prop:String) = configModule.properties.getProperty(prop)

  private def getProperty(prop:String, default:String) = configModule.properties.getProperty(prop,default)

  def outputConfig = {
    configModule.properties.stringPropertyNames().toArray.sortWith(_.toString() < _.toString()).foreach { name =>
      println(name.toString() + " = " + configModule.properties.getProperty(name.toString(), "NOT DEFINED"))
    }
  }
  //layer defaults
  val stateProvinceLayerID = configModule.properties.getProperty("layer.state.province", "")
  val terrestrialLayerID = configModule.properties.getProperty("layer.terrestrial", "")
  val marineLayerID = configModule.properties.getProperty("layer.marine", "")
  val countriesLayerID = configModule.properties.getProperty("layer.countries", "")
  val localGovLayerID = configModule.properties.getProperty("layer.localgov", "")

  //grid reference indexing
  val gridRefIndexingEnabled = BooleanUtils.toBoolean(configModule.properties.getProperty("gridref.indexing.enabled", "false"))

  //used by location processor for associating a country with an occurrence record where only stateProvince supplied
  val defaultCountry = configModule.properties.getProperty("default.country", "Australia")

  val versionProperties = {
    val properties = new Properties()
    //read manifest instead
    val resources = getClass().getClassLoader().getResources("META-INF/MANIFEST.MF")
    while (resources.hasMoreElements()) {
      try {
        val url = resources.nextElement()
        val manifest = new java.util.jar.Manifest(url.openStream())
        val title = manifest.getMainAttributes().get(new Attributes.Name("Implementation-Title"))
        if("Biocache".equals(title)){
          val entries:Attributes = manifest.getMainAttributes()
          entries.entrySet().foreach(entry => {
            properties.put(entry.getKey().toString, entry.getValue().toString)
          })
        }
      } catch {
        case e:Exception => e.printStackTrace()
      }
    }
    properties
  }

  val additionalFieldsToIndex = {
    var list = configModule.properties.getProperty("additional.fields.to.index", "").split(",").toList
    if (list.length == 1 && list(0).length == 0) {
      list = List()
    }
    list
  }

  // SDS URL
  val sdsUrl = configModule.properties.getProperty("sds.url", "https://sds.ala.org.au")

  val sdsLayersUrl = {
    val layersUrl = configModule.properties.getProperty("sds.layers.url", "")
    if (layersUrl == ""){
      sdsUrl + "/ws/layers"
    } else {
      layersUrl
    }
  }

  val sdsEnabled = configModule.properties.getProperty("sds.enabled", "true").toBoolean

  //load sensitive data service
  lazy val sdsFinder: SensitiveSpeciesFinder = synchronized {
    if (sdsEnabled) {
      SpatialLayerDAO
      SensitiveSpeciesFinderFactory.getSensitiveSpeciesFinder(nameIndex)
    } else {
      null
    }
  }

  //fields that should be hidden in certain views
  val sensitiveFields = {
    val configProps = configModule.properties.getProperty("sensitive.field", "")
    if(configProps == ""){
      Set("originalSensitiveValues","originalDecimalLatitude","originalDecimalLongitude", "originalLocationRemarks",
        "originalVerbatimLatitude", "originalVerbatimLongitude")
    } else {
      configProps.split(",").map(x => x.trim).toSet[String]
    }
  }

  val exportIndexAsCsvPath = configModule.properties.getProperty("export.index.as.csv.path", "")
  val exportIndexAsCsvPathSensitive = configModule.properties.getProperty("export.index.as.csv.path.sensitive", "")

  val caseSensitiveCassandra = configModule.properties.getProperty("cassandra.case.sensitive", "true").toBoolean
  val createColumnCassandra = configModule.properties.getProperty("cassandra.column.create", "true").toBoolean
}

/**
 * Guice configuration module.
 */
private class ConfigModule extends AbstractModule {

  protected val logger = LoggerFactory.getLogger("ConfigModule")

  val properties = {

    val properties = new Properties()
    //NC 2013-08-16: Supply the properties file as a system property via -Dbiocache.config=<file>
    //or the default /data/biocache/config/biocache-test-config.properties file is used.

    //check to see if a system property has been supplied with the location of the config file
    val filename = System.getProperty("biocache.config", "/data/biocache/config/biocache-config.properties")
    logger.info("Using config file: " + filename)

    val file = new java.io.File(filename)

    //only load the properties file if it exists otherwise default to the biocache-test-config.properties on the classpath
    val stream = if(file.exists()) {
      new FileInputStream(file)
    } else {
      this.getClass.getResourceAsStream(filename)
    }

    if(stream == null){
      throw new RuntimeException("Configuration file not found. Please add to classpath or /data/biocache/config/biocache-config.properties")
    }

    logger.debug("Loading configuration from " + filename)
    properties.load(stream)

    //this allows the SDS to access the same config file
    System.setProperty("sds.config.file", filename)

    properties
  }

  override def configure() {

    Names.bindProperties(this.binder, properties)
    //bind concrete implementations
    logger.debug("Initialising DAOs")
    bind(classOf[OccurrenceDAO]).to(classOf[OccurrenceDAOImpl]).in(Scopes.SINGLETON)
    bind(classOf[OutlierStatsDAO]).to(classOf[OutlierStatsDAOImpl]).in(Scopes.SINGLETON)
    bind(classOf[DeletedRecordDAO]).to(classOf[DeletedRecordDAOImpl]).in(Scopes.SINGLETON)
    bind(classOf[DuplicateDAO]).to(classOf[DuplicateDAOImpl]).in(Scopes.SINGLETON)
    bind(classOf[ValidationRuleDAO]).to(classOf[ValidationRuleDAOImpl]).in(Scopes.SINGLETON)
    bind(classOf[QidDAO]).to(classOf[QidDAOImpl]).in(Scopes.SINGLETON)
    logger.debug("Initialising SOLR")
    bind(classOf[IndexDAO]).to(classOf[SolrIndexDAO]).in(Scopes.SINGLETON)
    logger.debug("Initialising name matching indexes")
    try {
      val nameIndexLocation = properties.getProperty("name.index.dir")
      logger.debug("Loading name index from " + nameIndexLocation)
      val nameIndex = new ALANameSearcher(nameIndexLocation)
      bind(classOf[ALANameSearcher]).toInstance(nameIndex)
    } catch {
      case e: Exception => logger.warn("Lucene indexes are not currently available. " +
        "Please check 'name.index.dir' property in config. Message: " + e.getMessage())
    }
    logger.debug("Initialising persistence manager")
    properties.getProperty("db") match {
      case "mock" => bind(classOf[PersistenceManager]).to(classOf[MockPersistenceManager]).in(Scopes.SINGLETON)
//      case "cassandra" => bind(classOf[PersistenceManager]).to(classOf[CassandraPersistenceManager]).in(Scopes.SINGLETON)
      case "cassandra3" => bind(classOf[PersistenceManager]).to(classOf[Cassandra3PersistenceManager]).in(Scopes.SINGLETON)
      case _ => throw new RuntimeException("Persistence manager type unrecognised. Please check your external config file. ")
    }
    logger.debug("Configure complete")
  }
}
