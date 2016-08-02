package au.org.ala.biocache.tool

import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.index.SolrIndexDAO
import au.org.ala.biocache.util.OptionParser
import org.slf4j.LoggerFactory

/**
  * Created by mar759 on 31/07/2016.
  */
object IndexLocalRecords  extends Tool {

  def cmd = "index-local-node"
  def desc = "Index all records on a local node"

  def main(args:Array[String]){

    var threads:Int = 1
    var address:String = "127.0.0.1"
    var solrHome = "/data/solr/"
    var solrConfigXmlPath = "/data/solr/biocache/conf/solrconfig.xml"
    val parser = new OptionParser(help) {
      opt("local-ip", "local-ip-node", "The address", {v:String => address = v } )
      opt("sh", "solr-home",  "SOLR Home directory", {v:String => solrHome = v })
      opt("sc", "solr-config-path",  "SOLR Config XML file path", {v:String => solrConfigXmlPath = v })
      intOpt("t", "no-of-threads", "The number of threads to use", {v:Int => threads = v } )
    }
    if(parser.parse(args)){
      new IndexLocalRecords().indexRecords(threads, address, solrHome, solrConfigXmlPath)
    }
  }
}

/**
  * Created by mar759 on 29/07/2016.
  */
class IndexLocalRecords {

  val logger = LoggerFactory.getLogger("IndexLocalRecords")

  def indexRecords(threads: Int, address: String, solrHome:String, solrConfigXmlPath:String): Unit = {

    val start = System.currentTimeMillis()
    val indexer = new SolrIndexDAO(solrHome, Config.excludeSensitiveValuesFor, Config.extraMiscFields)
    indexer.solrConfigPath = solrConfigXmlPath

    val total = Config.persistenceManager.pageOverLocal("occ", (guid, map) => {
      try {
        indexer.indexFromMap(guid, map)
      } catch {
        case e:Exception => {
          logger.error("Problem indexing record: " + guid + " - "  + e.getMessage())
          logger.debug("Problem indexing record: " + guid + " - "  + e.getMessage(), e)
        }
      }
      true
    }, threads, address)
    val end = System.currentTimeMillis()
    logger.info("Total records indexed : " + total + " in " + ((end-start).toFloat / 1000f / 60f) + " minutes")

    indexer.commit()
    indexer.shutdown

    Config.persistenceManager.shutdown
  }
}


