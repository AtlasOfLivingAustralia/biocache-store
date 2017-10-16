package au.org.ala.biocache.tool

import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.index._
import au.org.ala.biocache.util.OptionParser

/**
  * A tool for indexing the local records of a node.
  */
object IndexLocalRecordsV2 extends Tool {

  def cmd = "index-local-node-v2"

  def desc = "Index all records on a local node"

  def main(args: Array[String]) {

    var threadsPerWriter = 2
    var threadsPerProcess = 4
    var writerCount = 2
    var ramPerWriter = 500
    var writerSegmentSize = 100
    var writerBufferSize = 1000
    var processorBufferSize = 1000
    var pageSize = 1000
    var mergeSegments = 2
    var test = false
    var testMap = false

    var threads = 2
    var solrHome = "/data/solr/"
    val checkpointFile = Config.tmpWorkDir + "/index-local-records-checkpoints.txt"
    var solrConfigXmlPath = solrHome + "/biocache/conf/solrconfig.xml"
    var zkc = Config.zookeeperAddress
    var optimise = true
    var optimiseOnly = false
    val parser = new OptionParser(help) {
      opt("zkc", "zk-config", "Zookeeper instance host:port to retrieve SOLR configuration from", { v: String => zkc = v })
      opt("sh", "solr-home", "SOLR home directory on the file system or the zookeeper host:port if rewriting directly to SOLR cloud instance", { v: String => solrHome = v })
      opt("sc", "solr-config-path", "SOLR Config XML file path", { v: String => solrConfigXmlPath = v })
      intOpt("t", "no-of-threads", "The number of threads to use", { v: Int => threads = v })
      opt("skip-optimise", "Optimise the new index once writing has completed. Defaults to true", {
        optimise = false
      })
      opt("test", "Test indexing. This skips the production of an index. Defaults to false", {
        test = true
      })
      opt("test-map", "Use map paging instead of array. Defaults to false", {
        testMap = true
      })
      opt("optimise-only", "Optimise the index once writing has completed", {
        optimise = true
        optimiseOnly = true
      })
      intOpt("ms", "mergesegments", "The number of output segments. No merge output is produced when 0. Default is " + mergeSegments, {
        v: Int => mergeSegments = v
      })
      intOpt("wc", "writercount", "The number of index writers. Default is " + writerCount, {
        v: Int => writerCount = v
      })
      intOpt("wt", "writerthreads", "The number of threads for each indexing writer. There is 1 writer for each -t. Default is " + threadsPerWriter, {
        v: Int => threadsPerWriter = v
      })
      intOpt("wb", "writerbuffer", "Size of indexing write buffer. The default is " + writerBufferSize, {
        v: Int => writerBufferSize = v
      })
      intOpt("pt", "processthreads", "The number of threads for each indexing process. There is 1 process for each -t. Default is " + threadsPerProcess, {
        v: Int => threadsPerProcess = v
      })
      intOpt("pb", "processbuffer", "Size of the indexing process buffer. Default is " + processorBufferSize, {
        v: Int => processorBufferSize = v
      })
      intOpt("r", "writerram", "Ram allocation for each writer (MB). There is 1 writer for each -t. Default is " + ramPerWriter, {
        v: Int => ramPerWriter = v
      })
      intOpt("ws", "writersegmentsize", "Maximum number of occurrences in a writer segment. There is 1 writer for each -t. Default is " + writerSegmentSize, {
        v: Int => writerSegmentSize = v
      })
      intOpt("ps", "pagesize", "The page size for the records. Default is " + pageSize, {
        v: Int => pageSize = v
      })
    }
    if (parser.parse(args)) {
      val ilr = new IndexLocalRecordsV2()
      //ZookeeperUtil.getSolrConfig(zkc, solrHome)
      ilr.indexRecords(threads, solrHome, solrConfigXmlPath, optimise, optimiseOnly, checkpointFile,
        threadsPerWriter, threadsPerProcess, ramPerWriter, writerSegmentSize, processorBufferSize,
        writerBufferSize, pageSize, mergeSegments, test, writerCount, testMap)
    }
  }
}