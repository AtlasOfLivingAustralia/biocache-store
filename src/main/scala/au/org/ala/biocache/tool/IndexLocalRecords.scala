package au.org.ala.biocache.tool

import java.io.File

import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.index.SolrIndexDAO
import au.org.ala.biocache.util.OptionParser
import org.apache.commons.io.FileUtils
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryOneTime
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
    var solrConfigXmlPath =  solrHome + "/biocache/conf/solrconfig.xml"
    var zkc = ""
    val parser = new OptionParser(help) {
      opt("local-ip", "local-ip-node", "The address", {v:String => address = v } )
      opt("zkc", "zk-config",  "Zookeeper instance host:port to retrieve SOLR configuration from", {v:String => zkc = v })
      opt("sh", "solr-home",  "SOLR home directory on the file system or the zookeeper host:port if rewriting directly to SOLR cloud instance", {v:String => solrHome = v })
      opt("sc", "solr-config-path",  "SOLR Config XML file path", {v:String => solrConfigXmlPath = v })
      intOpt("t", "no-of-threads", "The number of threads to use", {v:Int => threads = v } )
    }
    if(parser.parse(args)){
      val ilr = new IndexLocalRecords()
      if(zkc != ""){
        ilr.getZookeeperConfig(zkc)
      }
      ilr.indexRecords(threads, address, solrHome, solrConfigXmlPath)
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
          logger.error("Problem indexing record: " + guid + " - "  + e.getMessage(), e)
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


  def getZookeeperConfig(zookeeperHostPort:String): Unit ={

    import scala.collection.JavaConversions._

    println("Reading zookeeper config...")

    val builder = CuratorFrameworkFactory.builder()
    val curator = builder
      .namespace("configs")
      .retryPolicy(new RetryOneTime(1))
      .connectString(zookeeperHostPort).build()

    curator.start()

    val list = curator.getChildren.forPath("/biocache")
    val confDir = new File("/data/solr/biocache/conf")
    FileUtils.forceMkdir(confDir)
    val dataDir = new File("/data/solr/biocache/data")
    FileUtils.forceMkdir(dataDir)

    list.foreach(str => {
      val data:Array[Byte] = curator.getData().forPath("/biocache/" + str)
      val configFile = new String(data).map(_.toChar).toCharArray.mkString
      FileUtils.writeStringToFile(new File("/data/solr/biocache/conf/" + str), configFile)
    })

    FileUtils.writeStringToFile(new File("/data/solr/solr.xml"), "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><solr></solr>")
    FileUtils.writeStringToFile(new File("/data/solr/zoo.cfg"), "")

    FileUtils.writeStringToFile(new File("/data/solr/biocache/core.properties"), "name=biocache\nconfig=solrconfig.xml\nschema=schema.xml\ndataDir=data")
  }
}


