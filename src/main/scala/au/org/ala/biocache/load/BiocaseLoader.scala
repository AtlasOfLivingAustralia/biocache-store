package au.org.ala.biocache.load

import java.net.{URI, URL}
import java.util.concurrent.TimeUnit
import java.util.{Date, UUID}

import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.model.{Multimedia, Versions}
import au.org.ala.biocache.util.OptionParser
import com.google.common.base.Optional
import com.google.common.primitives.Bytes
import org.gbif.crawler.client.HttpCrawlClient
import org.gbif.crawler.protocol.biocase.{BiocaseCrawlConfiguration, BiocaseResponseHandler, BiocaseScientificNameRangeRequestHandler}
import org.gbif.crawler.retry.LimitedRetryPolicy
import org.gbif.crawler.strategy.{ScientificNameRangeCrawlContext, ScientificNameRangeStrategy}
import org.gbif.crawler.{AbstractCrawlListener, CrawlConfiguration, CrawlContext, Crawler}
import org.gbif.dwc.terms.{DcTerm, Term}
import org.gbif.wrangler.lock.NoLockFactory
import org.slf4j.{LoggerFactory, MDC}

import scala.collection.mutable.ListBuffer
import scala.xml._

object BiocaseLoader extends Tool {

  def cmd = "load-biocase"
  def desc = "Loads from a BioCASe endpoint."

  def main(args: Array[String]) {
    val LOG = LoggerFactory.getLogger(getClass)

    var dataResourceUid = ""
    var updateLastChecked = true
    var testFile = false
    var logRowKeys = false

    val parser = new OptionParser(help) {
      arg("data-resource-uid", "the data resource to import", { v: String => dataResourceUid = v })
      booleanOpt("u", "updateLastChecked", "true if it should update registry with last loaded date, otherwise use false", { v: Boolean => updateLastChecked = v })
      opt("test", "test the file only do not load into Cassandra", { testFile = true })
    }

    if (parser.parse(args)) {
      val l = new BiocaseLoader
      l.load(dataResourceUid, testFile)
      try {
        if (updateLastChecked) {
          l.updateLastChecked(dataResourceUid)
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }
}

class BiocaseLoader extends DataLoader {
  val LOG = LoggerFactory.getLogger(getClass)

  def load(dataResourceUid: String, test: Boolean) {

    val (endpoint, contentNamespace, datasetTitle) = retrieveConnectionParameters(dataResourceUid) match {
      case None => throw new Exception("Unable to retrieve connection params for " + dataResourceUid)
      case Some(config) => (
        new URI(config.urls.head),
        config.connectionParams.getOrElse("contentNamespace", ""),
        config.connectionParams.getOrElse("datasetTitle", "")
      )
    }

    val gbifID = UUID.randomUUID() // not used but required
    val attempt = 1 // not used but required
    val config = new BiocaseCrawlConfiguration(gbifID, attempt, endpoint, contentNamespace, datasetTitle);
    val context = new ScientificNameRangeCrawlContext()
    val strategy = new ScientificNameRangeStrategy(context,  ScientificNameRangeStrategy.Mode.ABC);

    val retryPolicy = new LimitedRetryPolicy(5, 2, 5, 2)
    val requestHandler = new BiocaseScientificNameRangeRequestHandler(config)
    val client = HttpCrawlClientProvider.newHttpCrawlClient(endpoint.getPort)
    try {
      val crawler = Crawler.newInstance(strategy, requestHandler, new BiocaseResponseHandler(), client, retryPolicy, NoLockFactory.getLock)

      val emit = (record: Map[String, String], multimedia: Seq[Multimedia]) => {
        val fr = FullRecordMapper.createFullRecord("", record, Versions.RAW)
        if (test) {
          // log something sensible to give confidence that records are interpreted properly
          LOG.info("Record: occurrenceID[{}], scientificName[{}], decimalLatitude[{}], decimalLongitude[{}]",
            fr.getOccurrence.getOccurrenceID, fr.getClassification.getScientificName,
            fr.getLocation.getDecimalLatitude, fr.getLocation.getDecimalLongitude);

          multimedia.foreach { media =>
            LOG.info("MediaItem: location[{}], created[{}], description[{}]",
              media.location,
              media.metadata.get(DcTerm.created.simpleName()),
              media.metadata.get(DcTerm.description.simpleName())
            )
          }
        } else {
          if (load(dataResourceUid, fr, List(fr.getOccurrence.getOccurrenceID), multimedia)) {
            LOG.debug("Successfully inserted: {}", fr.getOccurrence.getOccurrenceID);
          } else {
            LOG.error("Error inserting record");
          };
        }
      }

      // add the logging listener to aid diagnostics during operation
      crawler.addListener(new LoggingCrawlListener(config, null, null, 0, null).asInstanceOf[org.gbif.crawler.CrawlListener[ScientificNameRangeCrawlContext, String, java.util.List[java.lang.Byte]]])

      // add the listener which is responsible for emitting data into the biocache itself (i.e. the cassandra loading)
      crawler.addListener(new AlaBiocacheListener(emit).asInstanceOf[org.gbif.crawler.CrawlListener[ScientificNameRangeCrawlContext, String, java.util.List[java.lang.Byte]]])

      crawler.crawl()

      LOG.info("Finished crawling")
    } finally {
      client.shutdown()
    }
  }
}




/**
  * A listener that can be connected to the crawl stream.
  * For each page of results found, it will extract each record and call the emitter once with each record.
  * @param emit The emitter to use for each record
  */
class AlaBiocacheListener(emit: (Map[String, String], ListBuffer[Multimedia]) => Unit) extends AbstractCrawlListener[ScientificNameRangeCrawlContext, String, java.util.List[java.lang.Byte]] {

  val LOG = LoggerFactory.getLogger(getClass)

  override def response(
    response: java.util.List[java.lang.Byte],
    retry: Int,
    duration: Long,
    recordCount: Optional[java.lang.Integer],
    endOfRecords: Optional[java.lang.Boolean]): Unit = {

    LOG.info(f"recordCount: $recordCount, endOfRecords: $endOfRecords")

    // only waste time parsing if there are actually records
    if (0 < recordCount.get()) {
      val xmlResponseAsString = new String(Bytes.toArray(response))
      val xmlResponse = XML.loadString(xmlResponseAsString)
      //LOG.info("" + xmlResponseAsString);

      val units = xmlResponse \ "content" \ "DataSets" \\ "DataSet" \\ "Units"
      (units \\ "Unit").foreach { unit =>

        // Here we simply extract a few fields.  If this work is to be used in anger, we should discuss with
        // Jörg Holetshek and Tim Robertson a shared library to use between GBIF and the ALA.
        var recordAsDwC = Map(
          "occurrenceID" ->  (unit \ "UnitGUID").text,
          "decimalLatitude" -> (unit \\ "LatitudeDecimal").text,
          "decimalLongitude" -> (unit \\ "LongitudeDecimal").text,
          "scientificName" -> (unit \\ "FullScientificNameString").text,
          "institutionCode" -> (unit \\ "SourceInstitutionID").text,
          "collectionCode" -> (unit \\ "SourceID").text,
          "basisOfRecord" -> (unit \\ "RecordBasis").text,
          "eventDate" -> (unit \ "Gathering" \ "DateTime").text,
          "recordedBy" -> (unit \ "Gathering" \ "Agents" \ "GatheringAgent" \ "Person" \ "FullName").text,
          "locality" -> (unit \ "Gathering" \\ "LocalityText").text,
          "country" -> (unit \ "Gathering" \\ "Country" \\ "Name").text
        ).asInstanceOf[Map[String,String]];



        // multimedia items are handled in a special way
        val multimedia = new ListBuffer[Multimedia]
        var multimediaItems = unit \ "MultiMediaObjects";
        (multimediaItems \\ "MultiMediaObject").foreach { item =>
          val url = (item \ "FileURI").text;
          val metadata = Map(
            DcTerm.created -> (item \ "Creator").text,
            DcTerm.description -> (item \ "Context").text
          ).asInstanceOf[Map[Term, String]];
          multimedia += Multimedia.create(new URL(url), metadata)
        }


        //LOG.info("recordAsDwC: {}", recordAsDwC);

        emit(recordAsDwC, multimedia);
      }
    }
  }
}


/**
  * A simple listener that can be used to subscribe to the crawl stream and simply logs information about the status
  * of the crawl.  This is suitable for monitoring during production use.
  */
class LoggingCrawlListener(
  val configuration: CrawlConfiguration,
  var lastContext: CrawlContext,
  var lastRequest: String,

  var totalRecordCount: Int,

  var startDate: java.util.Date) extends AbstractCrawlListener[ScientificNameRangeCrawlContext, String, java.util.List[java.lang.Byte]] {

  val LOG = LoggerFactory.getLogger(getClass)

  MDC.put("datasetKey", configuration.getDatasetKey.toString)
  MDC.put("attempt", String.valueOf(configuration.getAttempt))

  override def error(msg: String) {
    LOG.warn("error during crawling: [{}], last request [{}], message [{}]", lastContext, lastRequest, msg)
  }

  override def error(e: Throwable) {
    LOG.warn("error during crawling: [{}], last request [{}]", lastContext, lastRequest, e)
  }

  override def finishCrawlAbnormally() {
    finishCrawl(FinishReason.ABORT)
  }

  override def finishCrawlNormally() {
    finishCrawl(FinishReason.NORMAL)
  }

  override def finishCrawlOnUserRequest() {
    finishCrawl(FinishReason.USER_ABORT)
  }

  override def progress(context: ScientificNameRangeCrawlContext) {
    lastRequest = null
    lastContext = context
    LOG.info(f"now beginning to crawl [$context]")
  }

  override def request(req: String, retry: Int) {
    LOG.info(f"requested page for [$lastContext], retry [$retry], request [$req]")
    lastRequest = req
  }

  override def response(
    response: java.util.List[java.lang.Byte],
    retry: Int,
    duration: Long,
    recordCount: Optional[java.lang.Integer],
    endOfRecords: Optional[java.lang.Boolean]): Unit = {
    totalRecordCount += recordCount.or(0)
    val took = TimeUnit.MILLISECONDS.toSeconds(duration)
    LOG.info(f"got response for [$lastContext], records [$recordCount], endOfRecords [$endOfRecords], retry [$retry], took [${took}s]")
  }

  override def startCrawl() {
    this.startDate = new Date()
    LOG.info("started crawl")
  }

  def finishCrawl(reason: FinishReason.Value) {
    val finishDate = new Date()
    val minutes = (finishDate.getTime - startDate.getTime) / (60 * 1000)
    LOG.info(
      f"finished crawling with a total of [$totalRecordCount] records, reason [$reason], started at [$startDate], finished at [$finishDate], took [$minutes] minutes")

    MDC.remove("datasetKey")
    MDC.remove("attempt")
  }

  object FinishReason extends Enumeration {
    type FinishReason = Value

    val NORMAL = Value("Normal")
    val USER_ABORT = Value("User Abort")
    val ABORT = Value("Abort")
    val UNKNOWN = Value("Unknown")
  }

}

/**
  * Provider of an HTTP crawl client configured with sensible values for production use.
  */
object HttpCrawlClientProvider {

  val DEFAULT_HTTP_PORT = 80
  val CONNECTION_TIMEOUT_MSEC = 600000 // 10 mins
  val MAX_TOTAL_CONNECTIONS = 10
  val MAX_TOTAL_PER_ROUTE = 3

  def newHttpCrawlClient(port: Int = -1): HttpCrawlClient = {
    HttpCrawlClient.newInstance(CONNECTION_TIMEOUT_MSEC, MAX_TOTAL_CONNECTIONS, MAX_TOTAL_PER_ROUTE)
  }

  def HttpCrawlClientProvider() {
    throw new UnsupportedOperationException("Can't initialize class")
  }

}



