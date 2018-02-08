package au.org.ala.biocache.tool

import java.io.File

import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.index.{SolrIndexDAO}
import au.org.ala.biocache.util.{ZookeeperUtil, OptionParser}
import org.apache.commons.io.FileUtils
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryOneTime
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * A tool for indexing the local records of a node.
  */
object IndexLocalRecords extends Tool {

  def cmd = "index-local-node-old"
  def desc = "Index all records on a local node"

  def main(args:Array[String]){

    var threads = 1
    var solrHome = "/data/solr/"
    val checkpointFile = Config.tmpWorkDir + "/index-local-records-checkpoints.txt"
    var solrConfigXmlPath = solrHome + "/biocache/conf/solrconfig.xml"
    var zkc = Config.zookeeperAddress
    var optimise = true
    var optimiseOnly = false
    val parser = new OptionParser(help) {
      opt("zkc", "zk-config",  "Zookeeper instance host:port to retrieve SOLR configuration from", { v:String => zkc = v })
      opt("sh", "solr-home",  "SOLR home directory on the file system or the zookeeper host:port if rewriting directly to SOLR cloud instance", {v:String => solrHome = v })
      opt("sc", "solr-config-path",  "SOLR Config XML file path", { v:String => solrConfigXmlPath = v })
      intOpt("t", "no-of-threads", "The number of threads to use", { v:Int => threads = v } )
      opt("skip-optimise", "Optimise the new index once writing has completed. Defaults to true", { optimise = false })
      opt("optimise-only", "Optimise the index once writing has completed", {
        optimise = true
        optimiseOnly = true
      })
    }
    if(parser.parse(args)){
      val ilr = new IndexLocalRecords()
      ZookeeperUtil.getSolrConfig(zkc, solrHome)
      ilr.indexRecords(threads, solrHome, solrConfigXmlPath, optimise, optimiseOnly, checkpointFile)
    }
  }
}

/**
  * A class for local records indexing.
  */
class IndexLocalRecords {

  val logger = LoggerFactory.getLogger("IndexLocalRecords")

  def indexRecords(threads: Int, solrHome:String, solrConfigXmlPath:String, optimise:Boolean, optimiseOnly:Boolean,
                   checkpointFile:String
                  ): Unit = {

    ZookeeperUtil.setStatus("INDEXING", "STARTING", 0)

    val start = System.currentTimeMillis()
    val indexer = new SolrIndexDAO(solrHome, Config.excludeSensitiveValuesFor, Config.extraMiscFields)
    indexer.solrConfigPath = solrConfigXmlPath

    System.setProperty("tokenRangeCheckPointFile", checkpointFile)

    if(new File(checkpointFile).exists()){

      logger.info("Checkpoint file detected. Will attempt to restart indexing process.....")
      val solrWriteLockFile = new File(solrHome + "/biocache/data/index/write.lock")
      if(solrWriteLockFile.exists()){
        logger.info("Removing SOLR lock file....")
        solrWriteLockFile.delete()
      }

      //do a facet on
      var indexedTokenRanges = ListBuffer[String]()
      indexer.pageOverFacet((tokenRangeIdx, count) => { indexedTokenRanges += tokenRangeIdx; true }, "batch_id_s", "*:*", Array())

      //completed token ranges
      var completedTokenRanges = ListBuffer[String]()
      Source.fromFile(checkpointFile).getLines().foreach { line =>
        val parts = line.split(",")
        if(parts.length == 2){
          completedTokenRanges += parts(0)
        }
      }

      System.setProperty("completedTokenRanges", completedTokenRanges.mkString(","))

      //clearing partially complete token ranges
      indexedTokenRanges.foreach { indexedTokenRange =>
        if(!completedTokenRanges.contains(indexedTokenRange)){
          //delete from index
          logger.info("Removing records in token range: " + indexedTokenRange)
          indexer.removeByQuery("batch_id_s:" + indexedTokenRange)
        }
      }
    }
    var count = 0

    if(!optimiseOnly) {

      val total = Config.persistenceManager.pageOverLocal("occ", (guid, map, tokenRangeIdx) => {
        try {




          indexer.indexFromMap(guid, map, batchID = tokenRangeIdx)



          count += 1
          if(count % 100000 == 0){
            ZookeeperUtil.setStatus("INDEXING", "INDEXING", count)
          }
        } catch {
          case e: Exception => {
            logger.error("Problem indexing record: " + guid + " - " + e.getMessage(), e)
          }
        }
        true
      }, threads, Array())

      indexer.finaliseIndex(optimise=false, shutdown = false)

      val end = System.currentTimeMillis()
      logger.info("Indexing completed. Total indexed : " + total + " in " + ((end - start).toFloat / 1000f / 60f) + " minutes")

      indexer.commit()
    }

    //Move checkpoint file if complete
    new File(checkpointFile).renameTo(new File(checkpointFile + ".complete"))

    if(optimise){
      ZookeeperUtil.setStatus("INDEXING", "OPTIMISING", count)
      logger.info("Optimising index....")
      indexer.optimise
      logger.info("Optimising complete.")
    } else {
      logger.info("Optimisation skipped....")
    }

    ZookeeperUtil.setStatus("INDEXING", "COMPLETE", count)
    indexer.shutdown

    Config.persistenceManager.shutdown
  }
}