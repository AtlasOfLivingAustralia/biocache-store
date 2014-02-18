package au.org.ala.biocache.qa
import java.text.MessageFormat
import scala.io.Source
import scala.util.parsing.json.JSON
import scala.collection.mutable.ArrayBuffer
import au.org.ala.biocache.Config
import java.io.FileWriter
import java.io.File
import au.org.ala.biocache.outliers.Timings
import org.codehaus.jackson.map.ObjectMapper
import org.slf4j.LoggerFactory
import au.org.ala.biocache.util.{Json, OptionParser, BiocacheConversions}
import au.org.ala.biocache.model.ValidationRule
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.index.IndexRecords

/**
 * Executable that applies the validation rules provided by users.
 */
object ValidationRuleRunner {

  def main(args: Array[String]) {

    var apiKey:Option[String] = None
    var id:Option[String] = None
    var unapply = false
    var reindex = false
    var tempFilePath = "/tmp/queryAssertionReindex.txt"
    val parser = new OptionParser("Application of Validation Rules") {
      opt("a", "apiKey","The apiKey whose assertions should be applied", {v:String => apiKey = Some(v)})
      opt("i","record id", "The uuid or the rowKey for the validation rule to apply", {v:String => id = Some(v)})
      opt("unapply", "Unapply the validation rules", {unapply=true})
      opt("reindex", "Reindex all the records for the validation rules", {reindex=true})
      opt("tempFilePath", "Location of a temporary file for validation rule outputs", {v:String => tempFilePath = v})
    }
    if(parser.parse(args)){      
      val runner = new ValidationRuleRunner
      if(apiKey.isDefined){
        runner.apply(apiKey.get, unapply, reindex, tempFilePath)
      } else if(id.isDefined){
        runner.applySingle(id.get, unapply, reindex, tempFilePath)
      }
      Config.persistenceManager.shutdown
      Config.indexDAO.shutdown
    } else {
      parser.showUsage
    }
    //new QueryAssertion().pageOverQuery("?q=lsid:urn:lsid:biodiversity.org.au:afd.taxon:1277dd43-882d-49d8-a009-2def58b0446e&wkt=MULTIPOLYGON(((144.75:-38,144.75:-37.75,145:-37.75,145.25:-37.75,145.25:-38,145:-38,144.75:-38)),((145.25:-38,145.5:-38,145.5:-38.25,145.25:-38.25,145.25:-38)),((145.25:-37.5,145.25:-37.25,145.5:-37.25,145.5:-37.5,145.25:-37.5)),((143.75:-37.75,143.75:-37.5,144:-37.5,144:-37.75,143.75:-37.75)))", Array(), {list => println(list);true;})
  }
}

class ValidationRuleRunner {

  import BiocacheConversions._
  val BIOCACHE_QUERY_URL = Config.biocacheServiceURL + "/occurrences/search{0}&facet=off{1}&pageSize={2}&startIndex={3}&fl=row_key"
  val logger = LoggerFactory.getLogger("ValidationRuleRunner")
  
  def applySingle(rowKey:String, unapply:Boolean=false, reindexOnly:Boolean=false, tempFilePath:String="/tmp/queryAssertionReindex.txt"){
    //get the query for the supplied key
    var buffer = new ArrayBuffer[String]
    def qa = Config.validationRuleDAO.get(rowKey)
    val filename = "/tmp/query_"+rowKey+"_reindex.txt"
    val reindexWriter = new FileWriter(filename)
    if(qa.isDefined){
      if(reindexOnly){
        buffer ++= qa.get.records
      } else if(unapply){
        modifyList(qa.get.records.toList, qa.get, buffer, false)
        Config.persistenceManager.deleteColumns(qa.get.id, "queryassert","records","lastApplied")
      } else {
        applyAssertion(qa.get,buffer)
      }
    }
    val rowKeySet = buffer.toSet[String]
    logger.debug("Row set : " + rowKeySet)
    rowKeySet.foreach(v => reindexWriter.write(v + "\n"))
    reindexWriter.flush
    reindexWriter.close
    IndexRecords.indexListThreaded(new File(filename), 4)
  }
  
  def apply(apiKey:String, unapply:Boolean=false, reindexOnly:Boolean=false, tempFilePath:String="/tmp/queryAssertionReindex.txt"){
    //val queryPattern = """\?q=([\x00-\x7F\s]*)&wkt=([\x00-\x7F\s]*)""".r
    val start = apiKey + "|"
    val end = start + "~"
    var buffer = new ArrayBuffer[String]

    val reindexWriter = new FileWriter(tempFilePath)
    logger.info("Starting...")
    Config.validationRuleDAO.pageOver(aq => {

      if(aq.isDefined){
        val assertion = aq.get
        if(reindexOnly){
          buffer ++= aq.get.records
        } else if(unapply){
          modifyList(assertion.records.toList, assertion, buffer, false)
          Config.persistenceManager.deleteColumns(assertion.id, "queryassert","records","lastApplied")
        } else {
          applyAssertion(assertion, buffer)
        }
      }
      true
    },start,end)

    //write the buffer out to file
    val rowKeySet = buffer.toSet[String]
    rowKeySet.foreach(v=>reindexWriter.write(v + "\n"))
    logger.debug("Row set : " + rowKeySet)
    reindexWriter.flush
    reindexWriter.close
    IndexRecords.indexListThreaded(new File(tempFilePath), 4)
  }
  
  def applyAssertion(assertion:ValidationRule, buffer:ArrayBuffer[String]){
    val timing = new Timings()
    val applicationDate:String = new java.util.Date()
    val newList = new ArrayBuffer[String]
    if(assertion.uuid == null){
      //need to create one
      assertion.uuid = Config.validationRuleDAO.createUuid
      Config.persistenceManager.put(assertion.id, "queryassert", "uuid", assertion.uuid)
    }
    if(assertion.wkt == null){
      //attempt to locate one from the raw query
      try {
        val om = new ObjectMapper()
        val validationRule = om.readValue(assertion.rawAssertion, classOf[ValidationRule])
        logger.debug(assertion.id + " " + validationRule.wkt)
        Config.persistenceManager.put(assertion.id, "queryassert","wkt",validationRule.wkt)
      } catch {
        case e:Exception => e.printStackTrace()
      }
    }
    if(assertion.disabled){
      //potentially need to remove the associated records
      modifyList(assertion.records.toList, assertion, buffer, false)
      Config.persistenceManager.deleteColumns(assertion.id, "queryassert","records")

    } else if(assertion.lastApplied != null && assertion.lastApplied.before(assertion.modifiedDate)){
      //need to reprocess all records and only add the records that haven't already been applied
      //remove records that have been applied but no longer exist
      pageOverQuery(assertion.rawQuery, Array[String](), { list =>
        newList ++= list
        true
      })
      def recSet = assertion.records.toSet[String]
      def newSet = newList.toSet[String]
      def removals = recSet diff newSet
      def additions = newSet diff recSet
      //remove records no longer part of the query assertion
      modifyList(removals.toList, assertion, buffer, false)
      //add records that a newly part of the query assertion
      modifyList(additions.toList, assertion,buffer, true)

      val props = Map("lastApplied" -> applicationDate,
        "records" -> Json.toJSON(newList.toArray[String].asInstanceOf[Array[AnyRef]])
      )
      Config.persistenceManager.put(assertion.id, "queryassert",props)

    } else {
      val fqs = {
         if(assertion.lastApplied == null){
           Array[String]()
         } else {
           val dateString:String = assertion.lastApplied
           Array("(last_processed_date:["+dateString+" TO *] OR last_load_date:["+dateString+" TO *])")
         }
      }
      pageOverQuery(assertion.rawQuery, fqs, { list =>
        newList ++= list
        true
      })

      //println(newList)
      modifyList(newList.toList, assertion, buffer, true)
      def set = (assertion.records.toList ++ newList).toSet[String]
      Config.persistenceManager.put(assertion.id, "queryassert", Map("lastApplied"->applicationDate,"records"->Json.toJSON(set.toArray[String].asInstanceOf[Array[AnyRef]])))
    }
    timing.checkpoint("Finished applying " + assertion.id)
  }
  
  /**
   * Either removes or add the query assertion to the supplied list.
   */
  def modifyList(list:List[String], aq:ValidationRule, buffer:ArrayBuffer[String], isAdd:Boolean){
    list.foreach(rowKey => {
      //check to see if it is already part of the assertion
      if((isAdd && !aq.records.contains(rowKey))||(!isAdd && aq.records.contains(rowKey))){
        //get the queryAssertions for the record
        val map  = Json.toJavaStringMap(Config.persistenceManager.get(rowKey, "occ", FullRecordMapper.queryAssertionColumn).getOrElse("{}"))
        if(isAdd){
          map.put(aq.uuid, aq.assertionType)
        } else {
          map.remove(aq.uuid)
        }
        //println(rowKey + " " + map)
        Config.persistenceManager.put(rowKey, "occ", FullRecordMapper.queryAssertionColumn,Json.toJSON(map))//.asScala[String,String].asInstanceOf[Map[String, Any]]))
        buffer += rowKey
      }
    })
  }
  
  def pageOverQuery(query:String, fqs:Array[String], proc:List[String]=>Boolean){
    var pageSize=500
    var startIndex=0
    var total = -1
    var filter = fqs.mkString("&fq=","&fq=","").replaceAll(" " ,"%20") 
    while(total == -1 || total > startIndex){
      val url = MessageFormat.format(BIOCACHE_QUERY_URL, query.replaceAll(" " ,"%20"),filter, pageSize.toString(), startIndex.toString())
      logger.info(url)
      val jsonString = Source.fromURL(url).getLines.mkString
      val json = JSON.parseFull(jsonString).get.asInstanceOf[Map[String, String]]
      //println(json)
      val results = json.get("occurrences").get.asInstanceOf[List[Map[String, String]]]
      val list = results.map(map => map.getOrElse("uuid",""))
      total = json.get("totalRecords").get.asInstanceOf[Double].toInt
      if(!proc(list)){
        startIndex = total
      }
      startIndex += pageSize
    }
  }
}