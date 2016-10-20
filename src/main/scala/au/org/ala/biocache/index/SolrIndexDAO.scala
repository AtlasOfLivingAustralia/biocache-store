package au.org.ala.biocache.index

import com.google.inject.Inject
import com.google.inject.name.Named
import org.slf4j.LoggerFactory
import org.apache.solr.core.CoreContainer
import org.apache.solr.client.solrj.{StreamingResponseCallback, SolrQuery, SolrServer}
import au.org.ala.biocache.dao.OccurrenceDAO
import org.apache.solr.common.{SolrDocument, SolrInputDocument}
import java.io.{FileWriter, OutputStream, File}
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer
import org.apache.solr.client.solrj.impl.{ConcurrentUpdateSolrServer}
import org.apache.solr.client.solrj.response.FacetField
import org.apache.solr.common.params.{MapSolrParams, ModifiableSolrParams}
import java.util.Date
import au.org.ala.biocache.parser.DateParser
import au.org.ala.biocache.Config
import au.org.ala.biocache.caches.TaxonSpeciesListDAO
import org.apache.commons.lang.StringUtils
import java.util.concurrent.ArrayBlockingQueue
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.vocab.{AssertionCodes, SpeciesGroups, ErrorCodeCategory}
import au.org.ala.biocache.util.Json

/**
  * DAO for indexing to SOLR
  */
class SolrIndexDAO @Inject()(@Named("solr.home") solrHome: String,
                             @Named("exclude.sensitive.values") excludeSensitiveValuesFor: String,
                             @Named("extra.misc.fields") defaultMiscFields: String) extends IndexDAO {

  import scala.collection.JavaConverters._
  import scala.collection.JavaConversions._

  override val logger = LoggerFactory.getLogger("SolrIndexDAO")

  val nameRegex = """(?:name":")([a-zA-z0-9]*)""".r
  val codeRegex = """(?:code":)([0-9]*)""".r
  val qaStatusRegex = """(?:qaStatus":)([0-9]*)""".r

  val arrDefaultMiscFields = if (defaultMiscFields == null) {
    Array[String]()
  } else {
    defaultMiscFields.split(",")
  }

  var cc: CoreContainer = _
  var solrServer: SolrServer = _
  var cloudServer: org.apache.solr.client.solrj.impl.CloudSolrServer = _
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

  override def init() {

    if (solrServer == null) {
      logger.info("Initialising the solr server " + solrHome + " cloudserver:" + cloudServer + " solrServer:" + solrServer)
      if(!solrHome.startsWith("http://")){
        if(solrHome.contains(":")) {
          //assume that it represents a SolrCloud
          cloudServer = new org.apache.solr.client.solrj.impl.CloudSolrServer(solrHome)
          cloudServer.setDefaultCollection("biocache1")
          solrServer = cloudServer
        } else if (solrConfigPath != "") {
          logger.info("Initialising embedded SOLR server.....")
          cc = CoreContainer.createAndLoad(solrHome, new File(solrHome+"/solr.xml"))
          solrServer = new EmbeddedSolrServer(cc, "biocache")
        } else {
          logger.info("Initialising embedded SOLR server.....")
          System.setProperty("solr.solr.home", solrHome)
          cc = CoreContainer.createAndLoad(solrHome,new File(solrHome + "/solr.xml"))//new CoreContainer(solrHome)
          solrServer = new EmbeddedSolrServer(cc, "biocache")
        }
      } else {
        logger.info("Initialising connection to SOLR server.....")
        solrServer = new ConcurrentUpdateSolrServer(solrHome, BATCH_SIZE, Config.solrUpdateThreads)
        logger.info("Initialising connection to SOLR server - done.")
      }
    }
  }

  def reload = if(cc != null) cc.reload("biocache")

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
    var values : java.util.List[FacetField.Count] = null

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

  def streamIndex(proc: java.util.Map[String,AnyRef] => Boolean, fieldsToRetrieve:Array[String], query:String, filterQueries: Array[String], sortFields: Array[String], multivaluedFields: Option[Array[String]] = None){

    init

    val params = collection.immutable.HashMap(
      "collectionName" -> "biocache",
      "q" -> query,
      "start" -> "0",
      "rows" -> Int.MaxValue.toString,
      "fl" -> fieldsToRetrieve.mkString(","))

    val solrParams = new ModifiableSolrParams()
    solrParams.add(new MapSolrParams(params))
    solrParams.add("fq", filterQueries:_*)

    if(!sortFields.isEmpty){
      solrParams.add("sort",sortFields.mkString(" asc,") + " asc")
    }

    //now stream
    val solrCallback = new SolrCallback(proc, multivaluedFields)
    logger.info("Starting to stream: " +new java.util.Date().toString + " " + params)
    solrServer.queryAndStreamResponse(solrParams, solrCallback)
    logger.info("Finished streaming : " +new java.util.Date().toString + " " + params)
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
        q.setSortField(sortField.get, if (dir == "asc") {
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
      logger.info("Deleting from index" + field +":" + value)
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
      logger.info(printNumDocumentsInIndex)
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

  def optimise : String = {
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
    */
  def shouldIndex(map: scala.collection.Map[String, String], startDate: Option[Date]): Boolean = {
    if (!startDate.isEmpty) {
      val lastLoaded = DateParser.parseStringToDate(getValue(FullRecordMapper.alaModifiedColumn, map))
      val lastProcessed = DateParser.parseStringToDate(getValue(FullRecordMapper.alaModifiedColumn + ".p", map))
      return startDate.get.before(lastProcessed.getOrElse(startDate.get)) || startDate.get.before(lastLoaded.getOrElse(startDate.get))
    }
    true
  }

  val multifields = Array("duplicate_inst", "establishment_means", "species_group", "assertions", "data_hub_uid", "interactions", "outlier_layer",
    "species_habitats", "multimedia", "all_image_url", "collectors", "duplicate_record", "duplicate_type","taxonomic_issue")

  val typeNotSuitableForModelling = Array("invalid", "historic", "vagrant", "irruptive")

  def extractPassAndFailed(json:String):(List[Int], List[(String,String)])={
    val codes = codeRegex.findAllMatchIn(json).map(_.group(1).toInt).toList
    val names = nameRegex.findAllMatchIn(json).map(_.group(1)).toList
    val qaStatuses = qaStatusRegex.findAllMatchIn(json).map(_.group(1)).toList
    val assertions = (names zip qaStatuses)
    if(logger.isDebugEnabled()){
      logger.debug("Codes:" + codes.toString)
      logger.debug("Name:" + names.toString)
      logger.debug("QA statuses:" + qaStatuses.toString)
      logger.debug("Assertions:" + assertions.toString)
    }
    (codes, assertions)
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
                            userProvidedTypeMiscIndexProperties : Seq[String] = Array[String](),
                            test:Boolean = false,
                            batchID:String = "",
                            csvFileWriter:FileWriter = null,
                            csvFileWriterSensitive:FileWriter = null) {
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

        val doc = new SolrInputDocument()
        for (i <- 0 to values.length - 1) {
          if (values(i) != "" && header(i) != "") {
            if (multifields.contains(header(i))) {
              //multiple values in this field
              for (value <- values(i).split('|')) {
                if (value != "") {
                  doc.addField(header(i), value)
                }
              }
            } else {
              doc.addField(header(i), values(i))
            }
          }
        }

        //add the misc properties here....
        //NC 2013-04-23: Change this code to support data types in misc fields.
        if (!miscIndexProperties.isEmpty) {
          val unparsedJson = map.getOrElse(FullRecordMapper.miscPropertiesColumn, "")
          if (unparsedJson != "") {
            val map = Json.toMap(unparsedJson)
            miscIndexProperties.foreach(prop => {
              prop match {
                case it if it.endsWith("_i") || it.endsWith("_d") || it.endsWith("_s") => {
                  val v = map.get(it.take(it.length-2))
                  if(v.isDefined && StringUtils.isNotBlank(it)){
                    doc.addField(it, v.get.toString())
                  }
                }

                case it if it.endsWith("_dt")  => {
                  val v = map.get(it.take(it.length-3))
                  if (v.isDefined && StringUtils.isNotBlank(it)) {
                    try {
                      val dateValue = DateParser.parseDate(v.get.toString())
                      if(!dateValue.isEmpty) {
                        doc.addField(it, dateValue.get.parsedStartDate)
                      } else {
                        logger.error("Unable to convert value to date " + v + " for " + guid)
                      }
                    }
                    catch {
                      case e:Exception => logger.error("Unable to convert value to date " + v + " for " + guid, e)
                    }
                  }
                }

                case _ => {
                  val v = map.get(prop)
                  if(v.isDefined){
                    doc.addField(prop + "_s", v.get.toString())
                  }
                }
              }
            })
          }
        }

        if (!userProvidedTypeMiscIndexProperties.isEmpty) {
          val unparsedJson = map.getOrElse(FullRecordMapper.miscPropertiesColumn, "")
          if (unparsedJson != "") {
            val map = Json.toMap(unparsedJson)
            userProvidedTypeMiscIndexProperties.foreach(prop => {
              prop match {
                case it if it.endsWith("_i") || it.endsWith("_d") || it.endsWith("_s") => {
                  val v = map.get(it)
                  if(v.isDefined && StringUtils.isNotBlank(it)){
                    doc.addField(it, v.get.toString())
                  }
                }

                case it if it.endsWith("_dt")  => {
                  val v = map.get(it)
                  if (v.isDefined && StringUtils.isNotBlank(it)) {
                    try {
                      val dateValue = DateParser.parseDate(v.get.toString())
                      if(!dateValue.isEmpty) {
                        doc.addField(it, dateValue.get.parsedStartDate)
                      } else {
                        logger.error("Unable to convert value to date " + v + " for " + guid)
                      }
                    }
                    catch {
                      case e:Exception => logger.error("Unable to convert value to date " + v + " for " + guid, e)
                    }
                  }
                }

                case _ => {
                  val v = map.get(prop)
                  if(v.isDefined){
                    doc.addField(prop, v.get.toString())
                  }
                }
              }
            })
          }
        }

        //add additional fields to index
        if (!Config.additionalFieldsToIndex.isEmpty) {
          val unparsedJson = map.getOrElse(FullRecordMapper.miscPropertiesColumn, "")
          if (unparsedJson != "") {
            val map = Json.toMap(unparsedJson)
            Config.additionalFieldsToIndex.foreach(prop => {
              val v = map.get(prop)
              if (v.isDefined) {
                doc.addField(prop, v.get.toString())
              }
            })
          }
        }

        if (!arrDefaultMiscFields.isEmpty) {
          val unparsedJson = map.getOrElse(FullRecordMapper.miscPropertiesColumn, "")
          if (unparsedJson != "") {
            val map = Json.toMap(unparsedJson)
            arrDefaultMiscFields.foreach(value => {
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
                      case e:Exception => logger.error("Unable to convert value to double " + fvalue + " for " + guid, e)
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
                      case e:Exception => logger.error("Unable to convert value to int " + fvalue + " for " + guid, e)
                    }
                  }
                }
                case datePattern(field) => {
                  val fvalue = map.getOrElse(field, "").toString()
                  if (fvalue.size > 0) {
                    try {
                      val dateValue = DateParser.parseDate(fvalue)
                      if(!dateValue.isEmpty) {
                        doc.addField(value, dateValue.get.parsedStartDate)
                      } else {
                        logger.error("Unable to convert value to date " + fvalue + " for " + guid)
                      }
                    }
                    catch {
                      case e:Exception => logger.error("Unable to convert value to date " + fvalue + " for " + guid, e)
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
            })
          }
        }

        //now index the System QA assertions
        //NC 2013-08-01: It is very inefficient to make a JSONArray of QualityAssertions We will parse the raw string instead.
        val qaJson  = map.getOrElse(FullRecordMapper.qualityAssertionColumn, "[]")
        val(qa, status) = extractPassAndFailed(qaJson)
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

        val unchecked = AssertionCodes.getMissingByCode(qa)
        unchecked.foreach(ec => doc.addField("assertions_unchecked", ec.name))

        doc.addField("system_assertions", sa)

        //load the species lists that are configured for the matched guid.
        val speciesLists = TaxonSpeciesListDAO.getCachedListsForTaxon(map.getOrElse("taxonConceptID.p",""))
        speciesLists.foreach { v =>
          doc.addField("species_list_uid", v)
        }

        /**
          * Additional indexing for grid references.
          * TODO refactor so that additional indexing is pluggable without core changes.
          */
        if(Config.gridRefIndexingEnabled){
          val bboxString = map.getOrElse("bbox.p", "")
          if(bboxString != ""){
            val bbox = bboxString.split(",")
            doc.addField("min_latitude", java.lang.Float.parseFloat(bbox(0)))
            doc.addField("min_longitude", java.lang.Float.parseFloat(bbox(1)))
            doc.addField("max_latitude", java.lang.Float.parseFloat(bbox(2)))
            doc.addField("max_longitude", java.lang.Float.parseFloat(bbox(3)))
          }

          val easting = map.getOrElse("easting.p", "")
          if(easting != "") doc.addField("easting", java.lang.Float.parseFloat(easting))
          val northing = map.getOrElse("northing.p", "")
          if(northing != "") doc.addField("northing", java.lang.Float.parseFloat(northing))
          val gridRef = map.getOrElse("gridReference", "")
          if(gridRef != "") {
            doc.addField("grid_ref", gridRef)

            if(gridRef.length() > 2) {

              val gridChars = gridRef.substring(0,2)
              val eastNorthing = gridRef.substring(2)
              val es = eastNorthing.splitAt((eastNorthing.length() / 2) )

              //add grid references for 10km, and 1km
              if (gridRef.length() >= 4) {
                doc.addField("grid_ref_10000", gridChars + es._1.substring(0,1)+ es._2.substring(0,1))
              }
              if (gridRef.length() == 5) {
                doc.addField("grid_ref_2000", gridRef)
              }
              if (gridRef.length() >= 6) {
                doc.addField("grid_ref_1000", gridChars + es._1.substring(0,2)+ es._2.substring(0,2))
              }
              if (gridRef.length() >= 8) {
                doc.addField("grid_ref_100", gridChars + es._1.substring(0,3)+ es._2.substring(0,3))
              }
            }
          }
        }
        /** UK NBN **/

        // user if userQA = true
        val hasUserAssertions = map.getOrElse(FullRecordMapper.userQualityAssertionColumn, "")
        if (!"".equals(hasUserAssertions)) {
          val assertionUserIds = Config.occurrenceDAO.getUserIdsForAssertions(guid)
          assertionUserIds.foreach(id => doc.addField("assertion_user_id", id))
        }

        val queryAssertions = Json.toStringMap(map.getOrElse(FullRecordMapper.queryAssertionColumn, "{}"))
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
        val els = Json.toStringMap(map.getOrElse("el.p", "{}"))
        els.foreach {
          case (key, value) => doc.addField(key, value)
        }
        val cls = Json.toStringMap(map.getOrElse("cl.p", "{}"))
        cls.foreach {
          case (key, value) => doc.addField(key, value)
        }

        //index the additional species information - ie species groups
        val lft = map.get("left.p")
        val rgt = map.get("right.p")
        if(lft.isDefined && rgt.isDefined){

          // add the species groups
          val sgs = SpeciesGroups.getSpeciesGroups(lft.get, rgt.get)
          if(sgs.isDefined){
            sgs.get.foreach{v:String => doc.addField("species_group", v)}
          }

          // add the species subgroups
          val ssgs = SpeciesGroups.getSpeciesSubGroups(lft.get, rgt.get)
          if(ssgs.isDefined){
            ssgs.get.foreach{v:String => doc.addField("species_subgroup", v)}
          }
        }

        if(batchID != ""){
          doc.addField("batch_id_s", batchID)
        }

        if(!test){
          if (!batch) {

            //if not a batch, add the doc and do a hard commit
            solrServer.add(doc)
            solrServer.commit(false, true, true)

            if (csvFileWriter != null) {
              writeDocToCsv(doc, csvFileWriter)
            }

            if (csvFileWriterSensitive != null) {
              writeDocToCsv(doc, csvFileWriterSensitive)
            }

          } else {

            currentBatch.synchronized {

              if (!StringUtils.isEmpty(values(0))){
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
                if (commit || currentCommitSize >= HARD_COMMIT_SIZE){
                  solrServer.commit(false, true, true)
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
        "batch_id_s" ) :::
      Config.fieldsToSample().toList

  lazy val csvHeaderSensitive = csvHeader.filterNot( h => sensitiveHeader.contains(h) )

  override def getCsvWriter(sensitive : Boolean = false) = {
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
    val header : List[String] = if (sensitive) { csvHeaderSensitive } else { csvHeader }

    fileWriter.write("\n")

    for (i <- 0 to header.length - 1) {
      val values = doc.getFieldValues(header.get(i))
      if (values != null && values.size() > 0) {
        var it = values.iterator();
        fileWriter.write(it.next().toString)
        while (it.hasNext) {
          fileWriter.write("|")
          fileWriter.write(it.next().toString)
        }
      }
      fileWriter.write("\t");
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
    val solrQuery = new SolrQuery();
    solrQuery.setQueryType("standard");
    // Facets
    solrQuery.setFacet(true)
    solrQuery.addFacetField("row_key")
    solrQuery.setQuery(query)
    solrQuery.setRows(0)
    solrQuery.setFacetLimit(limit)
    solrQuery.setFacetMinCount(1)
    try{
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
      case e:Exception => logger.warn("Unable to get key " + query+"."); None
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
    solrQuery.setQueryType("standard");
    // Facets
    solrQuery.setFacet(true)
    solrQuery.addFacetField("row_key")
    solrQuery.setQuery(query)
    solrQuery.setRows(0)
    solrQuery.setFacetLimit(limit)
    solrQuery.setFacetMinCount(1)
    try{
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
      case e:Exception => logger.warn("Unable to get key " + query+".");None
    }
  }

  def getDistinctValues(query: String, field: String, max: Int): Option[List[String]] = {
    init
    val solrQuery = new SolrQuery();
    solrQuery.setQueryType("standard");
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


  private def writeFieldToStream(field:String, query: String, outputStream: OutputStream) {
    init
    val size = 100
    var start = 0
    var continue = true

    val solrQuery = new SolrQuery()
      .setQueryType("standard")
      .setFacet(false)
      .setFields(field)
      .setQuery(query)
      .setRows(100)

    while (continue) {
      solrQuery.setStart(start)
      val response = solrServer.query(solrQuery)
      val resultsIterator = response.getResults().iterator
      while (resultsIterator.hasNext) {
        val result = resultsIterator.next()
        outputStream.write((result.getFieldValue(field) + "\n").getBytes())
      }

      start += size
      continue = response.getResults.getNumFound > start
    }
  }

  def printNumDocumentsInIndex(): String = {
    ">>>> Document count of index: " + solrServer.query(new SolrQuery("*:*")).getResults().getNumFound()
  }

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
              case e:Exception => logger.debug("Error committing to index", e) //do nothing
            }
          }
        } else {
          try {
            Thread.sleep(250)
          } catch {
            case e:Exception => logger.debug("Error sleeping thread", e) //do nothing
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
  class SolrCallback (proc: java.util.Map[String,AnyRef] => Boolean, multivaluedFields:Option[Array[String]]) extends StreamingResponseCallback {

    import scala.collection.JavaConverters._

    var maxResults = 0l
    var counter = 0l
    val start = System.currentTimeMillis
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis
    def streamSolrDocument(doc: SolrDocument) {
      val map = new java.util.HashMap[String, Object]
      doc.getFieldValueMap().keySet().asScala.foreach(s => {
        val value = if (multivaluedFields.isDefined && multivaluedFields.get.contains(s)){
          doc.getFieldValues(s)
        } else {
          doc.getFieldValue(s)
        }
        map.put(s, value)
      })
      proc(map)
      counter += 1
      if (counter % 10000 == 0){
        finishTime = System.currentTimeMillis
        logger.info(counter + " >> Last record : " + doc.getFieldValueMap + ", records per sec: " +
          10000.toFloat / (((finishTime - startTime).toFloat) / 1000f))
        startTime = System.currentTimeMillis
      }
    }

    def streamDocListInfo(numFound: Long, start: Long, maxScore: java.lang.Float) : Unit = {
      logger.info("NumFound: " + numFound +" start: " +start + " maxScore: " +maxScore)
      logger.info(new java.util.Date().toString)
      startTime = System.currentTimeMillis
      maxResults = numFound
    }
  }
}