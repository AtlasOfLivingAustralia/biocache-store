package au.org.ala.biocache.index

import java.io.{File, FileWriter, OutputStream}
import java.util.Date
import java.util.concurrent.ArrayBlockingQueue

import au.org.ala.biocache.Config
import au.org.ala.biocache.caches.TaxonSpeciesListDAO
import au.org.ala.biocache.dao.OccurrenceDAO
import au.org.ala.biocache.index.lucene.{DocBuilder, LuceneIndexing}
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.parser.DateParser
import au.org.ala.biocache.util.{GridUtil, Json}
import au.org.ala.biocache.vocab.{AssertionCodes, ErrorCode, ErrorCodeCategory, SpeciesGroups}
import com.datastax.driver.core.{ColumnDefinitions, GettableData}
import com.google.inject.Inject
import com.google.inject.name.Named
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer
import org.apache.solr.client.solrj.impl.{CloudSolrClient, ConcurrentUpdateSolrClient}
import org.apache.solr.client.solrj.response.FacetField
import org.apache.solr.client.solrj.{SolrClient, SolrQuery, StreamingResponseCallback}
import org.apache.solr.common.params.{CursorMarkParams, MapSolrParams, ModifiableSolrParams}
import org.apache.solr.common.{SolrDocument, SolrInputDocument, SolrInputField}
import org.apache.solr.core.CoreContainer
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * DAO for indexing to SOLR
  */
class SolrIndexDAO @Inject()(@Named("solr.home") solrHome: String,
                             @Named("exclude.sensitive.values") excludeSensitiveValuesFor: String,
                             @Named("extra.misc.fields") defaultMiscFields: String,
                             @Named("solr.collection") solrCollection: String = "biocache1") extends IndexDAO {

  import scala.collection.JavaConversions._
  import scala.collection.JavaConverters._

  override val logger = LoggerFactory.getLogger("SolrIndexDAO")

  val nameRegex = """(?:name":")([a-zA-z0-9]*)""".r
  val codeRegex = """(?:code":)([0-9]*)""".r
  val qaStatusRegex = """(?:qaStatus":)([0-9]*)""".r
  val userIdRegex = """(?:userId":)"([0-9]*)"""".r


  val arrDefaultMiscFields = if (defaultMiscFields == null) {
    Array[String]()
  } else {
    defaultMiscFields.split(",")
  }

  var cc: CoreContainer = _
  var solrServer: SolrClient = _
  var cloudServer: CloudSolrClient = _
  var solrConfigPath: String = ""

  @Inject
  var occurrenceDAO: OccurrenceDAO = _
  val currentBatch = new java.util.ArrayList[SolrInputDocument](1000)
  var currentCommitSize = 0
  var ids = 0
  val fieldSuffix = """([A-Za-z_\-0.9]*)"""
  val doublePattern = (fieldSuffix + """_d""").r
  val intPattern = (fieldSuffix + """_i""").r
  val datePattern = (fieldSuffix + """_dt""").r

  lazy val BATCH_SIZE = Config.solrBatchSize
  lazy val HARD_COMMIT_SIZE = Config.solrHardCommitSize
  val INDEX_READ_PAGE_SIZE = 5000
  val FACET_PAGE_SIZE = 1000

  lazy val drToExcludeSensitive = excludeSensitiveValuesFor.split(",")

  var luceneIndexing: LuceneIndexing = null
  var docBuilder: DocBuilder = null

  def init(columnDefinitions: ColumnDefinitions): Unit = {
    init()

    //
  }

  override def init() {

    if (luceneIndexing != null) {
      if (docBuilder == null) {
        docBuilder = luceneIndexing.getDocBuilder
      }
    } else {
      if (solrServer == null) {
        logger.info("Initialising the solr server " + solrHome + " cloudserver:" + cloudServer + " solrServer:" + solrServer)
        if (!solrHome.startsWith("http://")) {
          if (solrHome.contains(":")) {
            //assume that it represents a SolrCloud
            cloudServer = new CloudSolrClient.Builder().withZkHost(solrHome).build()
            cloudServer.setDefaultCollection(solrCollection)
            solrServer = cloudServer
            solrServer.ping()
          } else if (solrConfigPath != "") {
            logger.info("Initialising embedded SOLR server.....")
            cc = CoreContainer.createAndLoad(new File(solrHome).toPath, new File(solrHome + "/solr.xml").toPath)
            solrServer = new EmbeddedSolrServer(cc, "biocache")
          } else {
            logger.info("Initialising embedded SOLR server.....")
            System.setProperty("solr.solr.home", solrHome)
            cc = CoreContainer.createAndLoad(new File(solrHome).toPath, new File(solrHome + "/solr.xml").toPath) //new CoreContainer(solrHome)
            solrServer = new EmbeddedSolrServer(cc, "biocache")
          }
        } else {
          logger.info("Initialising connection to SOLR server..... with solrHome:  " + solrHome)
          solrServer = new ConcurrentUpdateSolrClient.Builder(solrHome).withThreadCount(Config.solrUpdateThreads).withQueueSize(BATCH_SIZE).build()
          logger.info("Initialising connection to SOLR server - done.")
        }
      }
    }
  }

  def reload = if (cc != null) cc.reload("biocache")

  override def shouldIncludeSensitiveValue(dr: String) = !drToExcludeSensitive.contains(dr)

  def pageOverFacet(proc: (String, Int) => Boolean, facetName: String,
                    queryString: String = "*:*", filterQueries: Array[String] = Array()) {

    init

    val query = new SolrQuery(queryString)
      .setFacet(true)
      .addFacetField(facetName)
      .setRows(0)
      .setFacetLimit(FACET_PAGE_SIZE)
      .setStart(0)
      .setFacetMinCount(1)

    filterQueries.foreach(query.addFilterQuery(_))

    var facetOffset = 0
    var values: java.util.List[FacetField.Count] = null

    do {
      query.remove("facet.offset")
      query.add("facet.offset", facetOffset.toString)

      val response = solrServer.query(query)
      values = response.getFacetField(facetName).getValues
      if (values != null) {
        values.asScala.foreach(s => proc(s.getName, s.getCount.toInt))
      }
      facetOffset += FACET_PAGE_SIZE

    } while (values != null && !values.isEmpty)
  }

  def streamIndex(proc: java.util.Map[String, AnyRef] => Boolean, fieldsToRetrieve: Array[String], query: String, filterQueries: Array[String], sortFields: Array[String], multivaluedFields: Option[Array[String]] = None) {

    init

    val params = collection.immutable.HashMap(
      "collectionName" -> "biocache",
      "q" -> query,
      "start" -> "0",
      "rows" -> Int.MaxValue.toString,
      "fl" -> fieldsToRetrieve.mkString(","))

    val solrParams = new ModifiableSolrParams()
    solrParams.add(new MapSolrParams(params))
    solrParams.add("fq", filterQueries: _*)

    if (!sortFields.isEmpty) {
      solrParams.add("sort", sortFields.mkString(" asc,") + " asc")
    }

    //now stream
    val solrCallback = new SolrCallback(proc, multivaluedFields)
    logger.info("Starting to stream: " + new java.util.Date().toString + " " + params)
    solrServer.queryAndStreamResponse(solrParams, solrCallback)
    logger.info("Finished streaming : " + new java.util.Date().toString + " " + params)
  }

  /**
    * Page over the index, handing off values to the supplied function.
    *
    * @param proc
    * @param fieldToRetrieve
    * @param queryString
    * @param filterQueries
    * @param sortField
    * @param sortDir
    * @param multivaluedFields
    */
  def pageOverIndex(proc: java.util.Map[String, AnyRef] => Boolean,
                    fieldToRetrieve: Array[String],
                    queryString: String = "*:*",
                    filterQueries: Array[String] = Array(),
                    sortField: Option[String] = None,
                    sortDir: Option[String] = None,
                    multivaluedFields: Option[Array[String]] = None) {
    init

    val query = new SolrQuery(queryString)
      .setFacet(false)
      .setRows(0)
      .setStart(0)
      .setFilterQueries(filterQueries: _*)
      .setFacet(false)

    fieldToRetrieve.foreach(f => query.addField(f))
    var response = solrServer.query(query)
    val fullResults = response.getResults.getNumFound.toInt
    logger.debug("Total found for :" + queryString + ", " + fullResults)

    var counter = 0
    var pageSize = INDEX_READ_PAGE_SIZE

    while (counter < fullResults) {

      val q = new SolrQuery(queryString)
        .setFacet(false)
        .setStart(counter)
        .setFilterQueries(filterQueries: _*)
        .setFacet(false)

      if (sortField.isDefined) {
        val dir = sortDir.getOrElse("asc")
        q.setSort(sortField.get, if (dir == "asc") {
          org.apache.solr.client.solrj.SolrQuery.ORDER.asc
        } else {
          org.apache.solr.client.solrj.SolrQuery.ORDER.desc
        })
      }

      if (counter + pageSize > fullResults) {
        pageSize = fullResults - counter
      }

      //setup the next query
      q.setRows(pageSize)
      response = solrServer.query(q)
      logger.info("Paging through :" + queryString + ", " + counter)
      val solrDocumentList = response.getResults
      val iter = solrDocumentList.iterator()
      while (iter.hasNext) {
        val solrDocument = iter.next()
        val map = new java.util.HashMap[String, Object]
        solrDocument.getFieldValueMap().keySet().asScala.foreach(s => map.put(s,
          if (multivaluedFields.isDefined && multivaluedFields.get.contains(s))
            solrDocument.getFieldValues(s)
          else
            solrDocument.getFieldValue(s))
        )
        proc(map)
      }
      counter += pageSize
    }
  }

  def pageOverIndexArray(proc: Array[AnyRef] => Boolean,
                         fieldToRetrieve: Array[String],
                         queryString: String = "*:*",
                         filterQueries: Array[String] = Array(),
                         sortField: Option[String] = None,
                         sortDir: Option[String] = None,
                         multivaluedFields: Option[Array[String]] = None) {
    init

    val query = new SolrQuery(queryString)
      .setFacet(false)
      .setRows(0)
      .setFilterQueries(filterQueries: _*)
      .setFacet(false)

    fieldToRetrieve.foreach(f => query.addField(f))
    var response = solrServer.query(query)
    val fullResults = response.getResults.getNumFound.toInt
    logger.debug("Total found for :" + queryString + ", " + fullResults)

    var counter = 0
    var pageSize = INDEX_READ_PAGE_SIZE

    var nextCursorMark = "*"
    while (counter < fullResults) {

      val q = new SolrQuery(queryString)
        .setFacet(false)
        .setFilterQueries(filterQueries: _*)
        .setFacet(false)
      q.set("cursorMark", nextCursorMark)

      if (sortField.isDefined) {
        val dir = sortDir.getOrElse("asc")
        q.setSort(sortField.get, if (dir == "asc") {
          org.apache.solr.client.solrj.SolrQuery.ORDER.asc
        } else {
          org.apache.solr.client.solrj.SolrQuery.ORDER.desc
        })
      }

      if (counter + pageSize > fullResults) {
        pageSize = fullResults - counter
      }

      //setup the next query
      q.setRows(pageSize)
      response = solrServer.query(q)
      logger.info("Paging through :" + queryString + ", " + counter)
      nextCursorMark = response.getNextCursorMark
      val solrDocumentList = response.getResults
      val iter = solrDocumentList.iterator()
      val fieldOrder = Array.fill[Int](fieldToRetrieve.length)(-1)
      val values = Array.fill[AnyRef](fieldToRetrieve.length)("")

      while (iter.hasNext) {
        val solrDocument = iter.next()
        val it = solrDocument.iterator()
        var i = 0
        while (it.hasNext) {
          val curr = it.next()
          if (fieldOrder(i) < 0) {
            //init fieldOrder value
            var p = 0
            while (fieldToRetrieve(p) != curr.getKey) {
              p = p + 1
            }
            fieldOrder(i) = p
          }
          if (fieldOrder(i) < values.length) {
            values(fieldOrder(i)) = curr.getValue
          } else {
            println("problem")
          }

          i = i + 1
        }
        proc(values)
      }
      counter += pageSize
    }
  }

  def emptyIndex {
    init
    try {
      solrServer.deleteByQuery("*:*")
    } catch {
      case e: Exception => logger.error("Problem clearing index...", e)
    }
  }

  def removeFromIndex(field: String, value: String) = {
    init
    try {
      logger.info("Deleting from index" + field + ":" + value)
      solrServer.deleteByQuery(field + ":\"" + value + "\"")
      solrServer.commit
    } catch {
      case e: Exception => logger.error("Problem removing from index...", e)
    }
  }

  def removeByQuery(query: String, commit: Boolean = true) = {
    init
    logger.info("Deleting by query: " + query)
    try {
      solrServer.deleteByQuery(query)
      if (commit)
        solrServer.commit
    } catch {
      case e: Exception => logger.error("Problem removing from index...", e)
    }
  }

  def finaliseIndex(optimise: Boolean = false, shutdown: Boolean = true) {
    init
    currentBatch.synchronized {
      if (!currentBatch.isEmpty) {
        solrServer.add(currentBatch)
        Thread.sleep(50)
      }
      logger.info("Performing index commit....")
      solrServer.commit
      currentCommitSize = 0
      logger.info("Performing index commit....done")
      currentBatch.clear
    }
    //clear the cache for the SpeciesLIst
    //now we should close the indexWriter
    logger.info(printNumDocumentsInIndex)
    if (optimise) {
      logger.info("Optimising the indexing...")
      this.optimise
    }
    if (shutdown) {
      logger.info("Shutting down the indexing...")
      this.shutdown
    }

    logger.info("Finalise finished.")
  }

  /**
    * Shutdown the index by stopping the indexing thread and shutting down the index core
    */
  def shutdown {
    //threads.foreach(t => t.stopRunning)
    if (cc != null)
      cc.shutdown
  }

  def optimise: String = {
    init
    solrServer.optimize
    printNumDocumentsInIndex
  }

  override def commit() {
    init
    solrServer.commit
  }

  /**
    * Decides whether or not the current record should be indexed based on processed times
    * and deletion status
    */
  def shouldIndex(map: scala.collection.Map[String, String], startDate: Option[Date]): Boolean = {
    if (map.getOrElse(FullRecordMapper.deletedColumn, "").length() > 0 || map.size < 2) {
      return false
    }
    if (!startDate.isEmpty) {
      val lastLoaded = DateParser.parseStringToDate(getValue(FullRecordMapper.alaModifiedColumn, map))
      val lastProcessed = DateParser.parseStringToDate(getValue(FullRecordMapper.alaModifiedColumn + Config.persistenceManager.fieldDelimiter + "p", map))
      return startDate.get.before(lastProcessed.getOrElse(startDate.get)) || startDate.get.before(lastLoaded.getOrElse(startDate.get))
    }
    true
  }

  def shouldIndex(array: GettableData, startDate: Option[Date]): Boolean = {
    if (getArrayValue(columnOrder.deletedColumn, array).length() > 0) {
      return false
    }
    if (!startDate.isEmpty) {
      val lastLoaded = DateParser.parseStringToDate(getArrayValue(columnOrder.alaModifiedColumn, array))
      val lastProcessed = DateParser.parseStringToDate(getArrayValue(columnOrder.alaModifiedColumnP, array))
      return startDate.get.before(lastProcessed.getOrElse(startDate.get)) || startDate.get.before(lastLoaded.getOrElse(startDate.get))
    }
    true
  }

  val multifields = Array("duplicate_inst", "establishment_means", "species_group", "assertions", "data_hub_uid", "interactions", "outlier_layer",
    "species_habitats", "multimedia", "all_image_url", "collectors", "duplicate_record", "duplicate_type", "taxonomic_issue")

  val typeNotSuitableForModelling = Array("invalid", "historic", "vagrant", "irruptive")

  def extractPassAndFailed(json: String): (List[Int], List[(String, String)]) = {
    val codes = codeRegex.findAllMatchIn(json).map(_.group(1).toInt).toList
    val names = nameRegex.findAllMatchIn(json).map(_.group(1)).toList
    val qaStatuses = qaStatusRegex.findAllMatchIn(json).map(_.group(1)).toList
    val assertions = (names zip qaStatuses)
    if (logger.isDebugEnabled()) {
      logger.debug("Codes:" + codes.toString)
      logger.debug("Name:" + names.toString)
      logger.debug("QA statuses:" + qaStatuses.toString)
      logger.debug("Assertions:" + assertions.toString)
    }
    (codes, assertions)
  }

  def extractUserIds(json: String): Set[String] = {
    userIdRegex.findAllMatchIn(json).map(_.group(1)).toSet
  }

  /**
    * A SOLR specific implementation of indexing from a map.
    */
  override def indexFromMap(guid: String,
                            map: scala.collection.Map[String, String],
                            batch: Boolean = true,
                            startDate: Option[Date] = None,
                            commit: Boolean = false,
                            miscIndexProperties: Seq[String] = Array[String](),
                            userProvidedTypeMiscIndexProperties: Seq[String] = Array[String](),
                            test: Boolean = false,
                            batchID: String = "",
                            csvFileWriter: FileWriter = null,
                            csvFileWriterSensitive: FileWriter = null) {
    init

    //val header = getHeaderValues()
    if (shouldIndex(map, startDate)) {

      val values = getOccIndexModel(guid, map)

      if (values.length > 0 && values.length != header.length) {
        logger.error("Values don't matcher header: " + values.length + ":" + header.length + ", values:header")
        logger.error("Headers: " + header.toString())
        logger.error("Values: " + values.toString())
        logger.error("This will be caused by changes in the list of headers not matching the number of submitted field values.")
        sys.exit(1)
      }

      if (!values.isEmpty) {

        val mapToIndex = new java.util.HashMap[String, SolrInputField]()

        for (i <- 0 to values.length - 1) {
          if (values(i) != "" && header(i) != "") {
            if (multifields.contains(header(i))) {
              //multiple values in this field
              val multiValuedField = new SolrInputField(header(i))
              for (value <- values(i).split('|')) {
                if (value != "") {
                  multiValuedField.addValue(value, 1.0f)
                }
              }
              mapToIndex.put(header(i), multiValuedField)

            } else {
              val singleValuedField = new SolrInputField(header(i))
              singleValuedField.setValue(values(i), 1.0f)
              mapToIndex.put(header(i), singleValuedField)
            }
          }
        }

        val doc = new SolrInputDocument(mapToIndex)

        //Sandbox dynamic field indexing - add the misc properties here....
        if (!miscIndexProperties.isEmpty) {
          val unparsedJson = getValue(FullRecordMapper.miscPropertiesColumn, map, "")
          if (unparsedJson != "") {
            val map = Json.toMap(unparsedJson)
            miscIndexProperties.foreach { prop =>
              prop match {
                case it if it.endsWith("_i") || it.endsWith("_d") || it.endsWith("_s") => {
                  val v = map.get(it.take(it.length - 2))
                  if (v.isDefined && StringUtils.isNotBlank(it)) {
                    doc.addField(it, v.get.toString())
                  }
                }

                case it if it.endsWith("_dt") => {
                  val v = map.get(it.take(it.length - 3))
                  if (v.isDefined && StringUtils.isNotBlank(it)) {
                    try {
                      val dateValue = DateParser.parseDate(v.get.toString())
                      if (!dateValue.isEmpty) {
                        doc.addField(it, dateValue.get.parsedStartDate)
                      } else {
                        logger.error("Unable to convert value to date " + v + " for " + guid)
                      }
                    }
                    catch {
                      case e: Exception => logger.error("Unable to convert value to date " + v + " for " + guid, e)
                    }
                  }
                }

                case _ => {
                  val v = map.get(prop)
                  if (v.isDefined) {
                    doc.addField(prop + "_s", v.get.toString())
                  }
                }
              }
            }
          }
        }

        // Configurable field indexing of misc properties
        if (!userProvidedTypeMiscIndexProperties.isEmpty) {
          val unparsedJson = getValue(FullRecordMapper.miscPropertiesColumn, map, "")
          if (unparsedJson != "") {
            val map = Json.toMap(unparsedJson)
            userProvidedTypeMiscIndexProperties.foreach { prop =>
              prop match {
                case it if it.endsWith("_i") || it.endsWith("_d") || it.endsWith("_s") => {
                  val v = map.get(it)
                  if (v.isDefined && StringUtils.isNotBlank(it)) {
                    doc.addField(it, v.get.toString())
                  }
                }

                case it if it.endsWith("_dt") => {
                  val v = map.get(it)
                  if (v.isDefined && StringUtils.isNotBlank(it)) {
                    try {
                      val dateValue = DateParser.parseDate(v.get.toString())
                      if (!dateValue.isEmpty) {
                        doc.addField(it, dateValue.get.parsedStartDate)
                      } else {
                        logger.error("Unable to convert value to date " + v + " for " + guid)
                      }
                    }
                    catch {
                      case e: Exception => logger.error("Unable to convert value to date " + v + " for " + guid, e)
                    }
                  }
                }

                case _ => {
                  val v = map.get(prop)
                  if (v.isDefined) {
                    doc.addField(prop, v.get.toString())
                  }
                }
              }
            }
          }
        }

        //add additional fields to index
        if (!Config.additionalFieldsToIndex.isEmpty) {
          val unparsedJson = getValue(FullRecordMapper.miscPropertiesColumn, map, "")
          if (unparsedJson != "") {
            val map = Json.toMap(unparsedJson)
            Config.additionalFieldsToIndex.foreach { prop =>
              val v = map.get(prop)
              if (v.isDefined) {
                doc.addField(prop, v.get.toString())
              }
            }
          }
        }

        if (!arrDefaultMiscFields.isEmpty) {
          val unparsedJson = getValue(FullRecordMapper.miscPropertiesColumn, map, "")
          if (unparsedJson != "") {
            val map = Json.toMap(unparsedJson)
            arrDefaultMiscFields.foreach { value =>
              value match {
                case doublePattern(field) => {
                  //ensure that the value represents a double value before adding to the index.
                  val fvalue = map.getOrElse(field, "").toString()
                  if (fvalue.size > 0) {
                    try {
                      java.lang.Double.parseDouble(fvalue)
                      doc.addField(value, fvalue)
                    }
                    catch {
                      case e: Exception => logger.error("Unable to convert value to double " + fvalue + " for " + guid, e)
                    }
                  }
                }
                case intPattern(field) => {
                  val fvalue = map.getOrElse(field, "").toString()
                  if (fvalue.size > 0) {
                    try {
                      java.lang.Integer.parseInt(fvalue)
                      doc.addField(value, fvalue)
                    }
                    catch {
                      case e: Exception => logger.error("Unable to convert value to int " + fvalue + " for " + guid, e)
                    }
                  }
                }
                case datePattern(field) => {
                  val fvalue = map.getOrElse(field, "").toString()
                  if (fvalue.size > 0) {
                    try {
                      val dateValue = DateParser.parseDate(fvalue)
                      if (!dateValue.isEmpty) {
                        doc.addField(value, dateValue.get.parsedStartDate)
                      } else {
                        logger.error("Unable to convert value to date " + fvalue + " for " + guid)
                      }
                    }
                    catch {
                      case e: Exception => logger.error("Unable to convert value to date " + fvalue + " for " + guid, e)
                    }
                  }
                }

                case _ => {
                  //remove the suffix
                  val item = if (value.contains("_")) value.substring(0, value.lastIndexOf("_")) else value
                  val fvalue = map.getOrElse(value, map.getOrElse(item, "")).toString()
                  if (fvalue.size > 0 && StringUtils.isNotBlank(value)) {
                    doc.addField(value, fvalue)
                  }
                }
              }
            }
          }
        }

        //now index the System QA assertions
        //NC 2013-08-01: It is very inefficient to make a JSONArray of QualityAssertions We will parse the raw string instead.
        val qaJson = getValue(FullRecordMapper.qualityAssertionColumn, map, "[]")
        val (qa, status) = extractPassAndFailed(qaJson)
        var sa = false
        status.foreach { case (test, status) =>
          if (status.equals("1")) {
            doc.addField("assertions_passed", test)
          } else if (status.equals("0")) {
            sa = true
            //get the error code to see if it is "missing"
            val assertionCode = AssertionCodes.getByName(test)

            def indexField = if (!assertionCode.isEmpty && assertionCode.get.category == ErrorCodeCategory.Missing) {
              "assertions_missing"
            } else {
              "assertions"
            }

            doc.addField(indexField, test)
          }
        }

        //index unchecked assertions
        val unchecked = AssertionCodes.getMissingByCode(qa)
        unchecked.foreach { ec => doc.addField("assertions_unchecked", ec.name) }

        // indicate if a record has system assertions
        doc.addField("system_assertions", sa)

        //Species lists - load the species lists that are configured for the matched guid.
        val taxonConceptID = getParsedValue("taxonConceptID", map)
        if (taxonConceptID != "") {
          val speciesLists = TaxonSpeciesListDAO.getCachedListsForTaxon(taxonConceptID)
          speciesLists.foreach { v =>
            doc.addField("species_list_uid", v)
          }
        }

        /**
          * Additional indexing for grid references.
          * TODO refactor so that additional indexing is pluggable without core changes.
          */
        if (Config.gridRefIndexingEnabled) {
          val bboxString = getParsedValue("bbox", map)
          if (bboxString != "") {
            val bbox = bboxString.split(",")
            doc.addField("min_latitude", java.lang.Float.parseFloat(bbox(0)))
            doc.addField("min_longitude", java.lang.Float.parseFloat(bbox(1)))
            doc.addField("max_latitude", java.lang.Float.parseFloat(bbox(2)))
            doc.addField("max_longitude", java.lang.Float.parseFloat(bbox(3)))
          }

          val easting = getParsedValue("easting", map)
          if (easting != "") doc.addField("easting", java.lang.Float.parseFloat(easting).toInt)
          val northing = getParsedValue("northing", map)
          if (northing != "") doc.addField("northing", java.lang.Float.parseFloat(northing).toInt)
          val gridRef = getValue("gridReference", map)
          if (gridRef != "") {
            doc.addField("grid_ref", gridRef)
            val map = GridUtil.getGridRefAsResolutions(gridRef)
            map.keySet.foreach { key => doc.addField(key, map.getOrElse(key, "")) }
          }
        }
        /** UK NBN **/

        // add a list of userIDs that have provided assertions
        val hasUserAssertions = getValue(FullRecordMapper.userQualityAssertionColumn, map)
        if (hasUserAssertions != "") {
          val assertionUserIds = extractUserIds(hasUserAssertions)
          assertionUserIds.foreach {  doc.addField("assertion_user_id", _) }
        }

        // add query assertions
        val queryAssertions = Json.toStringMap(getValue(FullRecordMapper.queryAssertionColumn, map, "{}"))
        var suitableForModelling = true
        queryAssertions.foreach {
          case (key, value) => {
            doc.addField("query_assertion_uuid", key)
            doc.addField("query_assertion_type_s", value)
            if (suitableForModelling && typeNotSuitableForModelling.contains(value))
              suitableForModelling = false
          }
        }

        //this will not exist for all records until a complete reindex is performed...
        doc.addField("suitable_modelling", suitableForModelling.toString)

        //index the available el and cl's - more efficient to use the supplied map than using the old way
        val els = Json.toStringMap(getParsedValue("el", map))
        els.foreach {
          case (key, value) => doc.addField(key, value)
        }
        val cls = Json.toStringMap(getParsedValue("cl", map))
        cls.foreach {
          case (key, value) => doc.addField(key, value)
        }
        
        
        //Species groups - index the additional species information - ie species groups
        val lft = getParsedValue("left", map)
        val rgt = getParsedValue("right", map)
        if (lft != "" && rgt != "") {

          // add the species groups
          val sgs = SpeciesGroups.getSpeciesGroups(lft, rgt)
          if (sgs.isDefined) {
            sgs.get.foreach { v: String => doc.addField("species_group", v) }
          }

          // add the species subgroups
          val ssgs = SpeciesGroups.getSpeciesSubGroups(lft, rgt)
          if (ssgs.isDefined) {
            ssgs.get.foreach { v: String => doc.addField("species_subgroup", v) }
          }
        }

        if (batchID != "") {
          doc.addField("batch_id_s", batchID)
        }

        if (!test) {
          if (!batch) {

            //if not a batch, add the doc and do a hard commit
            solrServer.add(doc, 10000)
            solrServer.commit(false, false, true)

            if (csvFileWriter != null) {
              writeDocToCsv(doc, csvFileWriter)
            }

            if (csvFileWriterSensitive != null) {
              writeDocToCsv(doc, csvFileWriterSensitive)
            }

          } else {

            currentBatch.synchronized {

              if (!StringUtils.isEmpty(values(0))) {
                currentBatch.add(doc)

                if (csvFileWriter != null) {
                  writeDocToCsv(doc, csvFileWriter)
                }

                if (csvFileWriterSensitive != null) {
                  writeDocToCsv(doc, csvFileWriterSensitive)
                }
              }

              if (currentBatch.size == BATCH_SIZE || (commit && !currentBatch.isEmpty)) {

                solrServer.add(currentBatch)
                currentCommitSize += currentBatch.size()
                if (commit || currentCommitSize >= HARD_COMMIT_SIZE) {
                  solrServer.commit(false, false, true)
                  currentCommitSize = 0
                }
                currentBatch.clear
              }
            }
          }
        }
      }
    }
  }

  /**
    * New indexing implementation
    * 
    * @param guid
    * @param map
    * @param batch
    * @param startDate
    * @param commit
    * @param miscIndexProperties
    * @param userProvidedTypeMiscIndexProperties
    * @param test
    * @param batchID
    * @param csvFileWriter
    * @param csvFileWriterSensitive
    * @param docBuilder
    * @param lock
    * @return
    */
  def indexFromMapNew(guid: String,
                      map: scala.collection.Map[String, String],
                      batch: Boolean = true,
                      startDate: Option[Date] = None,
                      commit: Boolean = false,
                      miscIndexProperties: Seq[String] = Array[String](),
                      userProvidedTypeMiscIndexProperties: Seq[String] = Array[String](),
                      test: Boolean = false,
                      batchID: String = "",
                      csvFileWriter: FileWriter = null,
                      csvFileWriterSensitive: FileWriter = null,
                      docBuilder: DocBuilder = null,
                      lock: Object = null): Long = {
    init

    var time = 0L

    if (shouldIndex(map, startDate)) {

      val doc = if (docBuilder == null) this.docBuilder else docBuilder

      try {
        doc.newDoc(guid)

        writeOccIndexModelToDoc(doc, guid, map)

        if (userProvidedTypeMiscIndexProperties.nonEmpty || miscIndexProperties.nonEmpty || arrDefaultMiscFields.nonEmpty
          || Config.additionalFieldsToIndex.nonEmpty) {
          val fieldsAndType: Map[String, String] = Map[String, String]()

          userProvidedTypeMiscIndexProperties.foreach(field =>
            if (!field.isEmpty) fieldsAndType.put(field.replaceAll("_[dsi(dt)]$", ""), field))

          miscIndexProperties.foreach(field =>
            if (!field.isEmpty) fieldsAndType.put(field.replaceAll("_[dsi(dt)]$", ""), field))

          arrDefaultMiscFields.foreach(field =>
            if (!field.isEmpty) fieldsAndType.put(field.replaceAll("_[dsi(dt)]$", ""), field))

          Config.additionalFieldsToIndex.foreach(field =>
            if (!field.isEmpty) fieldsAndType.put(field.replaceAll("_[dsi(dt)]$", ""), field))

          addJsonMapToDoc(doc, getValue(FullRecordMapper.miscPropertiesColumn, map), fieldsAndType, null, true)
        }

        addJsonArrayAssertionsToDoc(doc, getValue(FullRecordMapper.qualityAssertionColumn, map))

        //load the species lists that are configured for the matched guid.
        val speciesLists = TaxonSpeciesListDAO.getCachedListsForTaxon(getParsedValue("taxonConceptID", map))
        speciesLists.foreach { v =>
          doc.addField("species_list_uid", v)
        }

        /**
          * Additional indexing for grid references.
          */
        if (Config.gridRefIndexingEnabled) {
          val bboxString = getParsedValue("bbox", map)
          if (bboxString != "") {
            val bbox = bboxString.split(",")
            doc.addField("min_latitude", java.lang.Float.parseFloat(bbox(0)))
            doc.addField("min_longitude", java.lang.Float.parseFloat(bbox(1)))
            doc.addField("max_latitude", java.lang.Float.parseFloat(bbox(2)))
            doc.addField("max_longitude", java.lang.Float.parseFloat(bbox(3)))
          }

          val easting = getParsedValue("easting", map)
          if (easting != "") doc.addField("easting", java.lang.Float.parseFloat(easting).toInt)
          val northing = getParsedValue("northing", map)
          if (northing != "") doc.addField("northing", java.lang.Float.parseFloat(northing).toInt)
          val gridRef = getValue("gridReference", map)
          if (gridRef != "") {
            doc.addField("grid_ref", gridRef)
            val map = GridUtil.getGridRefAsResolutions(gridRef)
            map.keySet.foreach { key => doc.addField(key, map.getOrElse(key, "")) }
          }
        }
        /** UK NBN **/

        // user if userQA = true
        val hasUserAssertions = getValue(FullRecordMapper.userQualityAssertionColumn, map)
        if (StringUtils.isNotEmpty(hasUserAssertions)) {
          val assertionUserIds = extractUserIds(hasUserAssertions)
          assertionUserIds.foreach(id => doc.addField("assertion_user_id", id))
        }

        var suitableForModelling = addJsonMapToDoc(doc, getValue(FullRecordMapper.queryAssertionColumn, map), null, typeNotSuitableForModelling)

        //this will not exist for all records until a complete reindex is performed...
        doc.addField("suitable_modelling", suitableForModelling.toString)

        //index the available el and cl's - more efficient to use the supplied map than using the old way
        addJsonMapToDoc(doc, getParsedValue("el", map))
        addJsonMapToDoc(doc, getParsedValue("cl", map))

        //index the additional species information - ie species groups
        val lft = getParsedValue("left", map)
        val rgt = getParsedValue("right", map)
        if (!lft.isEmpty && !rgt.isEmpty) {

          // add the species groups
          val sgs = SpeciesGroups.getSpeciesGroups(lft, rgt)
          if (sgs.isDefined) {
            sgs.get.foreach { v: String => doc.addField("species_group", v) }
          }

          // add the species subgroups
          val ssgs = SpeciesGroups.getSpeciesSubGroups(lft, rgt)
          if (ssgs.isDefined) {
            ssgs.get.foreach { v: String => doc.addField("species_subgroup", v) }
          }
        }

        if (batchID != "") {
          doc.addField("batch_id_s", batchID)
        }

        if (!test) {
          val t1 = System.nanoTime()
          if (lock != null) {
            lock.synchronized {
              doc.index()
            }
          } else {
            doc.index()
          }
          time = System.nanoTime() - t1
        }

        if (csvFileWriter != null) {
          writeDocBuilderToCsv(doc, csvFileWriter)
        }

        if (csvFileWriterSensitive != null) {
          writeDocBuilderToCsv(doc, csvFileWriterSensitive)
        }
      } finally {
        //return the doc
        doc.release()
      }
    }
    time
  }

  def indexFromArray(guid: String,
                     array: GettableData,
                     columnDefinitions: ColumnDefinitions,
                     batch: Boolean = true,
                     startDate: Option[Date] = None,
                     commit: Boolean = false,
                     miscIndexProperties: Seq[String] = Array[String](),
                     userProvidedTypeMiscIndexProperties: Seq[String] = Array[String](),
                     test: Boolean = false,
                     batchID: String = "",
                     csvFileWriter: FileWriter = null,
                     csvFileWriterSensitive: FileWriter = null,
                     docBuilder: DocBuilder = null,
                     lock: Object = null): Long = {
    if (solrServer == null) {
      columnOrder.init(columnDefinitions, headerAttributes, headerAttributesFix, array_header_idx, array_header_parsed_idx, array_header_idx_fix, array_header_parsed_idx_fix)
    }

    init

    var time = 0L

    if (shouldIndex(array, startDate)) {

      val doc = if (docBuilder == null) this.docBuilder else docBuilder

      try {
        doc.newDoc(guid)

        writeOccIndexArrayToDoc(doc, guid, array)

        val fieldsAndType: Map[String, String] = Map[String, String]()
        if (userProvidedTypeMiscIndexProperties.nonEmpty || miscIndexProperties.nonEmpty || arrDefaultMiscFields.nonEmpty
          || Config.additionalFieldsToIndex.nonEmpty) {

          userProvidedTypeMiscIndexProperties.foreach(field =>
            if (!field.isEmpty) fieldsAndType.put(field.replaceAll("_[dsi(dt)]$", ""), field))

          miscIndexProperties.foreach(field =>
            if (!field.isEmpty) fieldsAndType.put(field.replaceAll("_[dsi(dt)]$", ""), field))

          arrDefaultMiscFields.foreach(field =>
            if (!field.isEmpty) fieldsAndType.put(field.replaceAll("_[dsi(dt)]$", ""), field))

          Config.additionalFieldsToIndex.foreach(field =>
            if (!field.isEmpty) fieldsAndType.put(field.replaceAll("_[dsi(dt)]$", ""), field))

          addJsonMapToDoc(doc, getArrayValue(columnOrder.miscPropertiesColumn, array), fieldsAndType, null, true)
        } else {
          //when indexing everything always add miscPropertiesColumn values to the doc
          addJsonMapToDoc(doc, getArrayValue(columnOrder.miscPropertiesColumn, array), fieldsAndType, null, true)
        }

        addJsonArrayAssertionsToDoc(doc, getArrayValue(columnOrder.qualityAssertionColumn, array))

        //load the species lists that are configured for the matched guid.
        val speciesLists = TaxonSpeciesListDAO.getCachedListsForTaxon(getArrayValue(columnOrder.taxonConceptIDP, array))
        speciesLists.foreach { v =>
          doc.addField("species_list_uid", v)
        }

        /**
          * Additional indexing for grid references.
          * TODO refactor so that additional indexing is pluggable without core changes.
          */
        if (Config.gridRefIndexingEnabled) {
          val bboxString = getArrayValue(columnOrder.bboxP, array)
          if (bboxString != "") {
            val bbox = bboxString.split(",")
            doc.addField("min_latitude", java.lang.Float.parseFloat(bbox(0)))
            doc.addField("min_longitude", java.lang.Float.parseFloat(bbox(1)))
            doc.addField("max_latitude", java.lang.Float.parseFloat(bbox(2)))
            doc.addField("max_longitude", java.lang.Float.parseFloat(bbox(3)))
          }

          val easting = getArrayValue(columnOrder.eastingP, array)
          if (easting != "") doc.addField("easting", java.lang.Float.parseFloat(easting).toInt)
          val northing = getArrayValue(columnOrder.northingP, array)
          if (northing != "") doc.addField("northing", java.lang.Float.parseFloat(northing).toInt)
          val gridRef = getArrayValue(columnOrder.gridReference, array)
          if (gridRef != "") {
            doc.addField("grid_ref", gridRef)
            val map = GridUtil.getGridRefAsResolutions(gridRef)
            map.keySet.foreach { key => doc.addField(key, map.getOrElse(key, "")) }
          }
        }
        /** UK NBN **/

        // user if userQA = true
        val hasUserAssertions = getArrayValue(columnOrder.userQualityAssertionColumn, array)
        if (StringUtils.isNotEmpty(hasUserAssertions)) {
          val assertionUserIds = Config.occurrenceDAO.getUserIdsForAssertions(getArrayValue(columnOrder.rowKey, array))
          assertionUserIds.foreach(id => doc.addField("assertion_user_id", id))
        }

        var suitableForModelling = addJsonMapToDoc(doc, getArrayValue(columnOrder.queryAssertionColumn, array), null, typeNotSuitableForModelling)

        //this will not exist for all records until a complete reindex is performed...
        doc.addField("suitable_modelling", suitableForModelling.toString)

        //index the available el and cl's - more efficient to use the supplied map than using the old way

        addJsonMapToDoc(doc, getArrayValue(columnOrder.elP, array))

        addJsonMapToDoc(doc, getArrayValue(columnOrder.clP, array))

        //index the additional species information - ie species groups

        val lft = getArrayValue(columnOrder.leftP, array)
        val rgt = getArrayValue(columnOrder.rightP, array)
        if (!lft.isEmpty && !rgt.isEmpty) {

          // add the species groups
          val sgs = SpeciesGroups.getSpeciesGroups(lft, rgt)
          if (sgs.isDefined) {
            sgs.get.foreach { v: String => doc.addField("species_group", v) }
          }

          // add the species subgroups
          val ssgs = SpeciesGroups.getSpeciesSubGroups(lft, rgt)
          if (ssgs.isDefined) {
            ssgs.get.foreach { v: String => doc.addField("species_subgroup", v) }
          }
        }

        if (batchID != "") {
          doc.addField("batch_id_s", batchID)
        }

        if (!test) {
          val t1 = System.nanoTime()
          if (lock != null) {
            lock.synchronized {
              doc.index()
            }
          } else {
            doc.index()
          }
          time = System.nanoTime() - t1
        }

        if (csvFileWriter != null) {
          writeDocBuilderToCsv(doc, csvFileWriter)
        }

        if (csvFileWriterSensitive != null) {
          writeDocBuilderToCsv(doc, csvFileWriterSensitive)
        }
      } finally {
        //return the doc
        doc.release()
      }
    }
    time
  }

  def addJsonMapToDoc(doc: DocBuilder, jsonString: String, fieldsAndType: Map[String, String] = null,
                      typeNotSuitableForModelling: Array[String] = null, addExtension: Boolean = false): Boolean = {
    var suitableForModelling: Boolean = true
    var start: Integer = 0
    var inVal = false
    var key: String = ""
    var skip: Boolean = false
    var skipped: Boolean = false
    var validKey: String = "valid"
    var index: Boolean = true
    var count = 0
    var c: Char = ' '
    for (i <- 0 until jsonString.length) {
      c = jsonString.charAt(i)
      if (skip) {
        skip = false
        skipped = true
      } else if (c == '\\') {
        skip = true
      } else if (jsonString.charAt(i) == '"') {
        if (!inVal) {
          inVal = true
          start = i + 1
        } else {
          inVal = false
          if (count % 2 == 0) {
            key = jsonString.substring(start, i)
            if (skipped) {
              skipped = false
              //parse
              key = StringEscapeUtils.unescapeJson(key)
            }

            if (fieldsAndType != null) {
              key = columnOrder.formatNameForSolr(key)

              validKey = fieldsAndType.getOrElse(key, "")
              index = true

              if (StringUtils.isEmpty(validKey)) {
                //when building SOLR doc, add everything use the default type of String and do not index by default
                // Add delimiter as prefix to avoid key conflicts with other SOLR fields
                key = "_" + key
                index = Config.solrIndexMisc
                validKey = "_s"
              }
            }
          } else {
            if (i - start > 1 && !validKey.isEmpty) {
              val value =
                if (skipped) {
                  //parse
                  StringEscapeUtils.unescapeJson(jsonString.substring(start, i))
                } else {
                  jsonString.substring(start, i)
                }

              if (typeNotSuitableForModelling != null) {
                doc.addField("query_assertion_uuid", key)
                doc.addField("query_assertion_type_s", value)
              } else {
                if (validKey.endsWith("_dt")) {
                  try {
                    val dateValue = DateParser.parseDate(value)
                    if (!dateValue.isEmpty) {
                      doc.addField(key, dateValue, index)
                    } else {
                      logger.error("Unable to convert value to date " + value + " for " + doc.getId())
                    }
                  }
                  catch {
                    case e: Exception => logger.error("Unable to convert value to date " + value + " for " + doc.getId())
                  }
                } else if (validKey.endsWith("_s") || validKey.endsWith("_d") || validKey.endsWith("_i")) {
                  doc.addField(key, value, index)
                } else if (addExtension) {
                  doc.addField(key + "_s", value, index)
                } else {
                  doc.addField(key, value, index)
                }
              }
              if (suitableForModelling && typeNotSuitableForModelling != null && typeNotSuitableForModelling.contains(value))
                suitableForModelling = false
            }
          }
          count = count + 1
        }
      }
    }
    suitableForModelling
  }

  def addJsonArrayAssertionsToDoc(doc: DocBuilder, jsonString: String) = {

    var i: Integer = 2
    var end: Integer = jsonString.length()
    var sa: Boolean = false

    var all: mutable.Set[ErrorCode] = mutable.Set[ErrorCode]()
    AssertionCodes.all.foreach(e => all.add(e))
    all.remove(AssertionCodes.PROCESSING_ERROR)
    all.remove(AssertionCodes.VERIFIED)

    while (end > 2) {
      end = jsonString.indexOf('{', i + 1)

      var codePos = jsonString.indexOf("\"code\":", i)
      var qaStatusPos = jsonString.indexOf("\"qaStatus\":", i)

      var code = ""
      if (codePos < end) {
        code = jsonString.substring(codePos + 7, jsonString.indexOf(',', codePos + 7))

        var qaStatus = ' '
        if (qaStatusPos < end) {
          qaStatus = jsonString.charAt(qaStatusPos + 11)

          val assertionCode = AssertionCodes.getByCode(code.toInt)
          if (qaStatus == '1') {
            doc.addField("assertions_passed", assertionCode.get.name)
          } else if (qaStatus == '0') {
            sa = true

            def indexField = if (!assertionCode.isEmpty && assertionCode.get.category == ErrorCodeCategory.Missing) {
              "assertions_missing"
            } else {
              "assertions"
            }

            doc.addField(indexField, assertionCode.get.name)
          }

          all.remove(assertionCode)
        }
      }

      i = end + 1
    }

    all.foreach(ec => doc.addField("assertions_unchecked", ec.name))

    doc.addField("system_assertions", sa)
  }


  //ignores "index-custom" additionalFields
  lazy val csvHeader =
    header :::
      arrDefaultMiscFields.toList :::
      List(
        FullRecordMapper.qualityAssertionColumn,
        FullRecordMapper.miscPropertiesColumn,
        "assertions_passed",
        "assertions_missing",
        "assertions",
        "assertions_unchecked",
        "system_assertions",
        "species_list_uid",
        "assertion_user_id",
        "query_assertion_uuid",
        "query_assertion_type_s",
        "suitable_modelling",
        "species_subgroup",
        "batch_id_s") :::
      Config.fieldsToSample().toList

  lazy val csvHeaderSensitive = csvHeader.filterNot(h => sensitiveHeader.contains(h))

  override def getCsvWriter(sensitive: Boolean = false) = {
    val fw = super.getCsvWriter(sensitive)
    if (sensitive) {
      fw.write(csvHeaderSensitive.mkString("\t"))
    } else {
      fw.write(csvHeader.mkString("\t"))
    }
    fw.write("\n")

    fw
  }

  def writeDocToCsv(doc: SolrInputDocument, fileWriter: FileWriter, sensitive: Boolean = false): Unit = {
    val header: List[String] = if (sensitive) {
      csvHeaderSensitive
    } else {
      csvHeader
    }

    fileWriter.write("\n")

    for (i <- 0 to header.length - 1) {
      val values = doc.getFieldValues(header.get(i))
      if (values != null && values.size() > 0) {
        val it = values.iterator()
        fileWriter.write(it.next().toString)
        while (it.hasNext) {
          fileWriter.write("|")
          fileWriter.write(it.next().toString)
        }
      }
      fileWriter.write("\t")
    }
  }

  def writeDocBuilderToCsv(docBuilder: DocBuilder, fileWriter: FileWriter, sensitive: Boolean = false): Unit = {
    val header: List[String] = if (sensitive) {
      csvHeaderSensitive
    } else {
      csvHeader
    }

    fileWriter.write("\n")

    val doc = docBuilder.getDoc()

    for (i <- 0 until header.length - 1) {
      val values = doc.get(header.get(i))
      if (values != null && values.length > 0) {
        for (j <- 0 until values.length) {
          if (j == 0) {
            fileWriter.write(values(j))
          } else {
            fileWriter.write("|")
            fileWriter.write(values(j))
          }
        }
      }
      fileWriter.write("\t")
    }
  }

  /**
    * Gets the rowKeys for the query that is supplied
    * Do here so that still works if web service is down
    *
    * This causes OOM exceptions at SOLR for large numbers of row keys
    * Use writeRowKeysToStream instead
    */
  override def getUUIDsForQuery(query: String, limit: Int = 1000): Option[List[String]] = {

    init
    val solrQuery = new SolrQuery()
    // Facets
    solrQuery.setFacet(true)
    solrQuery.addFacetField("row_key")
    solrQuery.setQuery(query)
    solrQuery.setRows(0)
    solrQuery.setFacetLimit(limit)
    solrQuery.setFacetMinCount(1)
    try {
      val response = solrServer.query(solrQuery)
      logger.debug("Query " + solrQuery.toString)
      //now process all the values that are in the row_key facet
      val rowKeyFacets = response.getFacetField("id")
      val values = rowKeyFacets.getValues().asScala
      if (values.size > 0) {
        Some(values.map(facet => facet.getName).toList)
      } else {
        None
      }
    } catch {
      case e: Exception => logger.warn("Unable to get key " + query + "."); None
    }
  }

  /**
    * Gets the rowKeys for the query that is supplied
    * Do here so that still works if web service is down
    *
    * This causes OOM exceptions at SOLR for large numbers of row keys
    * Use writeRowKeysToStream instead
    */
  override def getRowKeysForQuery(query: String, limit: Int = 1000): Option[List[String]] = {

    init
    val solrQuery = new SolrQuery();
    // Facets
    solrQuery.setFacet(true)
    solrQuery.addFacetField("row_key")
    solrQuery.setQuery(query)
    solrQuery.setRows(0)
    solrQuery.setFacetLimit(limit)
    solrQuery.setFacetMinCount(1)
    try {
      val response = solrServer.query(solrQuery)
      logger.debug("Query " + solrQuery.toString)
      //now process all the values that are in the row_key facet
      val rowKeyFacets = response.getFacetField("row_key")
      val values = rowKeyFacets.getValues().asScala
      if (values.size > 0) {
        Some(values.map(facet => facet.getName).toList)
      } else {
        None
      }
    } catch {
      case e: Exception => logger.warn("Unable to get key " + query + "."); None
    }
  }

  def getDistinctValues(query: String, field: String, max: Int): Option[List[String]] = {
    init
    val solrQuery = new SolrQuery();
    // Facets
    solrQuery.setFacet(true)
    solrQuery.addFacetField(field)
    solrQuery.setQuery(query)
    solrQuery.setRows(0)
    solrQuery.setFacetLimit(max)
    solrQuery.setFacetMinCount(1)
    val response = solrServer.query(solrQuery)
    val facets = response.getFacetField(field)
    //TODO page through the facets to make more efficient.
    if (facets.getValues() != null && !facets.getValues().isEmpty()) {
      val values = facets.getValues().asScala
      if (values != null && !values.isEmpty) {
        /*
          NC: Needed to change this method after the upgrade as it now throws a cast exception
          old value: Some(values.map(facet => facet.getName).asInstanceOf[List[String]])
         */
        Some(values.map(facet => facet.getName).toList)
      } else {
        None
      }
    } else {
      None
    }
  }

  /**
    * Writes the list of row_keys for the results of the specified query to the
    * output stream.
    */
  override def writeUUIDsToStream(query: String, outputStream: OutputStream) =
    writeFieldToStream("id", query, outputStream)

  /**
    * Writes the list of row_keys for the results of the specified query to the
    * output stream.
    */
  override def writeRowKeysToStream(query: String, outputStream: OutputStream) =
    writeFieldToStream("row_key", query, outputStream)


  private def writeFieldToStream(field: String, query: String, outputStream: OutputStream) {

    init
    var done = false
    val q = new SolrQuery()
      .setFacet(false)
      .setFields(field)
      .setQuery(query)
      .setRows(1000)
      .setSort("id", SolrQuery.ORDER.desc)

    var cursorMark = CursorMarkParams.CURSOR_MARK_START
    while (!done) {
      q.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark)
      val response = solrServer.query(q)
      val nextCursorMark = response.getNextCursorMark()
      val resultsIterator = response.getResults().iterator
      while (resultsIterator.hasNext) {
        val result = resultsIterator.next()
        outputStream.write((result.getFieldValue(field) + "\n").getBytes())
      }

      if (cursorMark.equals(nextCursorMark)) {
        done = true
      }
      cursorMark = nextCursorMark
    }
    outputStream.flush()
  }

  def printNumDocumentsInIndex =
    ">>>> Document count of index: " + solrServer.query(new SolrQuery("*:*")).getResults().getNumFound()

  class AddDocThread(queue: ArrayBlockingQueue[java.util.List[SolrInputDocument]], id: Int) extends Thread {

    private var shouldRun = true

    def stopRunning {
      shouldRun = false
    }

    override def run() {
      logger.info("Starting AddDocThread thread....")
      while (shouldRun || queue.size > 0) {
        if (queue.size > 0) {
          var docs = queue.poll()
          //add and commit the docs
          if (docs != null && !docs.isEmpty) {
            try {
              logger.info("Thread " + id + " is adding " + docs.size + " documents to the index.")
              solrServer.add(docs)
              //only the first thread should commit
              if (id == 0) {
                solrServer.commit(false, true, true)
              }
              docs = null
            } catch {
              case e: Exception => logger.debug("Error committing to index", e) //do nothing
            }
          }
        } else {
          try {
            Thread.sleep(250)
          } catch {
            case e: Exception => logger.debug("Error sleeping thread", e) //do nothing
          }
        }
      }
      logger.info("Finishing AddDocThread thread.")
    }
  }

  /**
    * Streaming callback for use with SOLR's streaming API.
    *
    * @param proc
    * @param multivaluedFields
    */
  class SolrCallback(proc: java.util.Map[String, AnyRef] => Boolean, multivaluedFields: Option[Array[String]]) extends StreamingResponseCallback {

    import scala.collection.JavaConverters._

    var maxResults = 0l
    var counter = 0l
    val start = System.currentTimeMillis
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis

    def streamSolrDocument(doc: SolrDocument) {
      val map = new java.util.HashMap[String, Object]
      doc.getFieldValueMap().keySet().asScala.foreach(s => {
        val value = if (multivaluedFields.isDefined && multivaluedFields.get.contains(s)) {
          doc.getFieldValues(s)
        } else {
          doc.getFieldValue(s)
        }
        map.put(s, value)
      })
      proc(map)
      counter += 1
      if (counter % 10000 == 0) {
        finishTime = System.currentTimeMillis
        logger.info(counter + " >> Last record : " + doc.getFieldValueMap + ", records per sec: " +
          10000.toFloat / (((finishTime - startTime).toFloat) / 1000f))
        startTime = System.currentTimeMillis
      }
    }

    def streamDocListInfo(numFound: Long, start: Long, maxScore: java.lang.Float): Unit = {
      logger.info("NumFound: " + numFound + " start: " + start + " maxScore: " + maxScore)
      logger.info(new java.util.Date().toString)
      startTime = System.currentTimeMillis
      maxResults = numFound
    }
  }

}

class ColumnOrder {

  def formatNameForSolr(name: String): String = {
    //format SOLR field names
    var formatted = ""
    var prev_was_upper = true
    if (!name.endsWith(Config.persistenceManager.fieldDelimiter + "qa")) {
      name.toCharArray.foreach(c => {
        if (!c.isLetterOrDigit) {
          formatted += "_"
        } else if (c.isUpper) {
          if (prev_was_upper) {
            formatted += c.toLower
          } else {
            formatted += "_" + c.toLower
          }
          prev_was_upper = true
        } else {
          formatted += c
          prev_was_upper = false
        }
      })
    }
    formatted
  }

  def init(columnDefinitions: ColumnDefinitions, headerAttributes: List[(String, String, Int, Int)], headerAttributesFix: List[(String, String, Int, Int)], array_header_idx: Array[Integer], array_header_parsed_idx: Array[Integer], array_header_idx_fix: Array[Integer], array_header_parsed_idx_fix: Array[Integer]) = {
    this.rowKey = columnDefinitions.getIndexOf("rowkey")
    this.taxonConceptIDP = columnDefinitions.getIndexOf("taxonConceptID" + Config.persistenceManager.fieldDelimiter + "p")
    this.deletedColumn = columnDefinitions.getIndexOf(FullRecordMapper.deletedColumn)
    this.alaModifiedColumn = columnDefinitions.getIndexOf(FullRecordMapper.alaModifiedColumn)
    this.alaModifiedColumnP = columnDefinitions.getIndexOf(FullRecordMapper.alaModifiedColumnP)
    this.miscPropertiesColumn = columnDefinitions.getIndexOf(FullRecordMapper.miscPropertiesColumn)
    this.qualityAssertionColumn = columnDefinitions.getIndexOf(FullRecordMapper.qualityAssertionColumn)
    this.decimalLatitudeP = columnDefinitions.getIndexOf("decimalLatitude" + Config.persistenceManager.fieldDelimiter + "p")
    this.decimalLongitudeP = columnDefinitions.getIndexOf("decimalLongitude" + Config.persistenceManager.fieldDelimiter + "p")
    this.scientificNameP = columnDefinitions.getIndexOf("scientificName" + Config.persistenceManager.fieldDelimiter + "p")
    this.vernacularNameP = columnDefinitions.getIndexOf("vernacularName" + Config.persistenceManager.fieldDelimiter + "p")
    this.kingdomP = columnDefinitions.getIndexOf("kingdom" + Config.persistenceManager.fieldDelimiter + "p")
    this.familyP = columnDefinitions.getIndexOf("family" + Config.persistenceManager.fieldDelimiter + "p")
    this.images = columnDefinitions.getIndexOf("images")
    this.sounds = columnDefinitions.getIndexOf("sounds")
    this.videos = columnDefinitions.getIndexOf("videos")
    this.interactionsP = columnDefinitions.getIndexOf("interactions" + Config.persistenceManager.fieldDelimiter + "p")
    this.yearP = columnDefinitions.getIndexOf("year" + Config.persistenceManager.fieldDelimiter + "p")
    this.dataResourceUid = columnDefinitions.getIndexOf("dataResourceUid")
    this.originalSensitiveValues = columnDefinitions.getIndexOf("originalSensitiveValues")
    this.countryConservationP = columnDefinitions.getIndexOf("countryConservation" + Config.persistenceManager.fieldDelimiter + "p")
    this.dataGeneralizationsP = columnDefinitions.getIndexOf("dataGeneralizations" + Config.persistenceManager.fieldDelimiter + "p")
    this.outlierForLayersP = columnDefinitions.getIndexOf("outlierForLayers" + Config.persistenceManager.fieldDelimiter + "p")
    this.geospatialDecisionColumn = columnDefinitions.getIndexOf(FullRecordMapper.geospatialDecisionColumn)
    this.userAssertionStatusColumn = columnDefinitions.getIndexOf(FullRecordMapper.userAssertionStatusColumn)
    this.taxonRankIDP = columnDefinitions.getIndexOf("taxonRankID" + Config.persistenceManager.fieldDelimiter + "p")
    this.scientificNameP = columnDefinitions.getIndexOf("scientificName" + Config.persistenceManager.fieldDelimiter + "p")
    this.vernacularNameP = columnDefinitions.getIndexOf("vernacularName" + Config.persistenceManager.fieldDelimiter + "p")
    this.kingdomP = columnDefinitions.getIndexOf("kingdom" + Config.persistenceManager.fieldDelimiter + "p")
    this.familyP = columnDefinitions.getIndexOf("family" + Config.persistenceManager.fieldDelimiter + "p")
    this.images = columnDefinitions.getIndexOf("images")
    this.sounds = columnDefinitions.getIndexOf("sounds")
    this.videos = columnDefinitions.getIndexOf("videos")
    this.outlierForLayersP = columnDefinitions.getIndexOf("outlierForLayers" + Config.persistenceManager.fieldDelimiter + "p")
    this.interactionsP = columnDefinitions.getIndexOf("interactions" + Config.persistenceManager.fieldDelimiter + "p")
    this.yearP = columnDefinitions.getIndexOf("year" + Config.persistenceManager.fieldDelimiter + "p")
    this.scientificName = columnDefinitions.getIndexOf("scientificName")
    this.genus = columnDefinitions.getIndexOf("genus")
    this.family = columnDefinitions.getIndexOf("family")
    this.specificEpithet = columnDefinitions.getIndexOf("specificEpithet")
    this.species = columnDefinitions.getIndexOf("species")
    this.infraspecificEpithet = columnDefinitions.getIndexOf("infraspecificEpithet")
    this.subspecies = columnDefinitions.getIndexOf("subspecies")

    this.stateConservationP = columnDefinitions.getIndexOf("stateConservation" + Config.persistenceManager.fieldDelimiter + "p")
    this.countryConservationP = columnDefinitions.getIndexOf("countryConservation" + Config.persistenceManager.fieldDelimiter + "p")
    this.taxonRankIDP = columnDefinitions.getIndexOf("taxonRankID" + Config.persistenceManager.fieldDelimiter + "p")
    this.informationWithheldP = columnDefinitions.getIndexOf("informationWithheld" + Config.persistenceManager.fieldDelimiter + "p")
    this.dataGeneralizationsP = columnDefinitions.getIndexOf("dataGeneralizations" + Config.persistenceManager.fieldDelimiter + "p")
    this.originalSensitiveValues = columnDefinitions.getIndexOf("originalSensitiveValues")
    this.userQualityAssertionColumn = columnDefinitions.getIndexOf(FullRecordMapper.userQualityAssertionColumn)
    this.bboxP = columnDefinitions.getIndexOf("bboxP")
    this.eastingP = columnDefinitions.getIndexOf("easting" + Config.persistenceManager.fieldDelimiter + "p")
    this.northingP = columnDefinitions.getIndexOf("northing" + Config.persistenceManager.fieldDelimiter + "p")
    this.gridReference = columnDefinitions.getIndexOf("gridReference")
    this.queryAssertionColumn = columnDefinitions.getIndexOf(FullRecordMapper.queryAssertionColumn)
    this.elP = columnDefinitions.getIndexOf("el" + Config.persistenceManager.fieldDelimiter + "p")
    this.clP = columnDefinitions.getIndexOf("cl" + Config.persistenceManager.fieldDelimiter + "p")
    this.rowKey = columnDefinitions.getIndexOf("rowkey")
    this.leftP = columnDefinitions.getIndexOf("left" + Config.persistenceManager.fieldDelimiter + "p")
    this.rightP = columnDefinitions.getIndexOf("right" + Config.persistenceManager.fieldDelimiter + "p")

    val isUsed: Array[Boolean] = new Array[Boolean](columnDefinitions.size())
    val columnNames: Array[String] = new Array[String](columnDefinitions.size())
    (0 until isUsed.length).foreach(i => {
      isUsed(i) = false
      columnNames(i) = formatNameForSolr(columnDefinitions.getName(i))
    })

    (0 until columnNames.length).foreach(i => {
      val name = columnNames(i)

      if (!name.endsWith(Config.persistenceManager.fieldDelimiter + "p")) {
        //add _raw to fields there is also a _p version
        if (columnDefinitions.contains(name + Config.persistenceManager.fieldDelimiter + "p")) {
          columnNames(i) = "raw_" + name
        }
      } else {
        //remove _p
        columnNames(i) = name.substring(0, name.length - 2)
      }
    })

    val fields = this.getClass.getDeclaredFields()
    (0 until fields.length).foreach(i => {
      val f = fields(i)
      if (f.getType.getName == "int") {
        f.setAccessible(true)
        val v = f.getInt(this)
        if (v >= columnDefinitions.size())
          System.out.println("ERROR not a valid occ column: " + f.getName)
        else if (v < 0)
          System.out.println("ERROR missing occ column: " + f.getName)
        if (v >= 0)
          isUsed(v) = true
      }
    })

    (0 until headerAttributes.length).foreach(i => {
      array_header_idx(i) = columnDefinitions.getIndexOf(headerAttributes(i)._1)
      array_header_parsed_idx(i) = columnDefinitions.getIndexOf(headerAttributes(i)._1 + Config.persistenceManager.fieldDelimiter + "p")

      if (array_header_idx(i) >= 0)
        isUsed(array_header_idx(i)) = true
      if (array_header_parsed_idx(i) >= 0)
        isUsed(array_header_parsed_idx(i)) = true
    })

    //TODO: remvoe when headerAttributesFix is not longer required
    (0 until headerAttributesFix.length).foreach(i => {
      array_header_idx_fix(i) = columnDefinitions.getIndexOf(headerAttributesFix(i)._1)
      array_header_parsed_idx_fix(i) = columnDefinitions.getIndexOf(headerAttributesFix(i)._1 + Config.persistenceManager.fieldDelimiter + "p")

      if (array_header_idx_fix(i) >= 0)
        isUsed(array_header_idx_fix(i)) = true
      if (array_header_parsed_idx_fix(i) >= 0)
        isUsed(array_header_parsed_idx_fix(i)) = true
    })

    this.columnNames = columnNames
    this.isUsed = isUsed
    this.length = columnDefinitions.size()
  }

  def getValue(idx: Integer, array: GettableData, default: String = ""): String = {
    if (idx >= 0) {
      var value = array.getString(idx)
      if (StringUtils.isEmpty(value)) {
        value = default
      }
      value
    } else {
      default
    }
  }

  var dataGeneralizationsP: Int = -1

  var originalSensitiveValues: Int = -1

  var dataResourceUid: Int = -1

  var informationWithheldP: Int = -1

  var taxonRankIDP: Int = -1

  var userAssertionStatusColumn: Int = -1

  var geospatialDecisionColumn: Int = -1

  var stateConservationP: Int = -1
  var countryConservationP: Int = -1

  var scientificName: Int = -1
  var genus: Int = -1
  var family: Int = -1
  var specificEpithet: Int = -1
  var species: Int = -1
  var infraspecificEpithet: Int = -1
  var subspecies: Int = -1

  var interactionsP: Int = -1

  var scientificNameP: Int = -1
  var kingdomP: Int = -1
  var familyP: Int = -1
  var vernacularNameP: Int = -1

  var yearP: Int = -1

  var outlierForLayersP: Int = -1

  var videos: Int = -1
  var sounds: Int = -1

  var images: Int = -1

  var decimalLongitudeP: Int = -1

  var decimalLatitudeP: Int = -1

  var rowKey: Int = -1

  var eastingP: Int = -1
  var northingP: Int = -1
  var queryAssertionColumn: Int = -1

  var userQualityAssertionColumn: Int = -1
  var elP: Int = -1
  var clP: Int = -1
  var leftP: Int = -1
  var rightP: Int = -1

  var bboxP: Int = -1

  var taxonConceptIDP: Int = -1

  var alaModifiedColumnP: Int = -1

  var alaModifiedColumn: Int = -1

  var deletedColumn: Int = -1

  var gridReference: Int = -1

  var qualityAssertionColumn: Int = -1
  var miscPropertiesColumn: Int = -1

  var isUsed: Array[Boolean] = _
  var columnNames: Array[String] = _
  var length: Long = 0L
}