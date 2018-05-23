package au.org.ala.biocache.index

import java.io.File

import au.org.ala.biocache._
import au.org.ala.biocache.index.lucene.{DocBuilder, LuceneIndexing}
import org.apache.commons.io.{FileUtils}
import org.apache.solr.core.{SolrConfig, SolrResourceLoader}
import org.apache.solr.schema.{IndexSchema, IndexSchemaFactory}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * A class for local record indexing.
  */
class IndexLocalNode {

  val logger: Logger = LoggerFactory.getLogger("IndexLocalNode")

  /**
    * Method for running localised indexing (localised to node) against cassandra.
    *
    * @param numThreads number of threads to use
    * @param solrHome SOLR home path to use
    * @param solrConfigXmlPath SOLR Config XML file path
    * @param optimise whether to optimise the index on completion
    * @param optimiseOnly run only optimise
    * @param checkpointFile checkpoint file path
    * @param threadsPerWriter number of threads to use per writer
    * @param threadsPerProcess
    * @param ramPerWriter
    * @param writerSegmentSize
    * @param processorBufferSize
    * @param writerBufferSize
    * @param pageSize
    * @param mergeSegments
    * @param test
    * @param writerCount
    * @param testMap
    * @param maxRecordsToIndex
    *
    * @return total number of records indexed.
    */
  def indexRecords(numThreads: Int,
                   solrHome: String,
                   solrConfigXmlPath: String,
                   optimise: Boolean,
                   optimiseOnly: Boolean,
                   checkpointFile: String,
                   threadsPerWriter: Int,
                   threadsPerProcess: Int,
                   ramPerWriter: Int,
                   writerSegmentSize: Int,
                   processorBufferSize: Int,
                   writerBufferSize: Int,
                   pageSize: Int,
                   mergeSegments: Int,
                   test: Boolean,
                   writerCount: Int,
                   testMap: Boolean,
                   maxRecordsToIndex:Int = -1
                  ) : Int = {

    val start = System.currentTimeMillis()

    System.setProperty("tokenRangeCheckPointFile", checkpointFile)

    var count = 0
    val singleWriter = writerCount == 0

    val (schema, schemaFile, newIndexDir, confDir, sourceConfDir) = setUpSolrConfig(solrHome: String, solrConfigXmlPath: String)

    // Setup indexers
    var luceneIndexing: Seq[LuceneIndexing] = initialiseIndexers(threadsPerWriter, ramPerWriter,
      writerSegmentSize, writerBufferSize, writerCount, schema, newIndexDir)

    if (test) {
      DocBuilder.setIsIndexing(false)
    }

    val counter: Counter = new DefaultCounter()
    if (testMap) {
      new IndexRunnerMap(
        counter,
        confDir,
        pageSize,
        luceneIndexing,
        threadsPerProcess,
        processorBufferSize,
        singleWriter,
        test,
        numThreads
      ).run()
    } else {
      new IndexRunner(
        counter,
        confDir,
        pageSize,
        luceneIndexing,
        threadsPerProcess,
        processorBufferSize,
        singleWriter,
        test,
        numThreads,
        maxRecordsToIndex
      ).run()
    }

    val end = System.currentTimeMillis()
    logger.info("Indexing completed in " + ((end - start).toFloat / 1000f / 60f) + " minutes")

    val dirs = new ArrayBuffer[String]()

    if (singleWriter) {
      luceneIndexing(0).close(true, false)
    }

    for (i <- luceneIndexing.indices) {
      for (j <- 0 until luceneIndexing(i).getOutputDirectories.size()) {
        dirs += luceneIndexing(i).getOutputDirectories.get(j).getPath
      }
    }

    //remove references
    luceneIndexing = null

    System.gc()

    val mem = Math.max((Runtime.getRuntime.freeMemory() * 0.75) / 1024 / 1024, writerCount * ramPerWriter).toInt

    writeAdditionalSchemaEntries(schemaFile, sourceConfDir)

    performMerge(solrHome, optimise, mergeSegments, schemaFile, dirs, mem)

    logger.info("Indexing complete. Records indexed: " + counter.counter)

    //Move checkpoint file if complete
    new File(checkpointFile).renameTo(new File(checkpointFile + ".complete"))

    counter.counter
  }

  /**
    * Initialise a set of indexers.
    *
    * @param threadsPerWriter
    * @param ramPerWriter
    * @param writerSegmentSize
    * @param writerBufferSize
    * @param writerCount
    * @param schema
    * @param newIndexDir
    * @return
    */
  private def initialiseIndexers(threadsPerWriter: Int, ramPerWriter: Int, writerSegmentSize: Int, writerBufferSize: Int,
                                 writerCount: Int,  schema: IndexSchema, newIndexDir: File) : Seq[LuceneIndexing] = {
    if (writerCount == 0) {
      val outputDir = newIndexDir.getParent + "/data0-"
      logger.info("Writing index to " + outputDir)
      List(new LuceneIndexing(schema, writerSegmentSize.toLong, outputDir,
        ramPerWriter, writerBufferSize, writerBufferSize / 2, threadsPerWriter))
    } else {
      val buff = new ArrayBuffer[LuceneIndexing]()
      for (i <- 0 until writerCount) {
        val outputDir = newIndexDir.getParent + "/data" + i + "-"
        logger.info("Writing index to " + outputDir)
        buff += new LuceneIndexing(schema, writerSegmentSize.toLong, outputDir,
          ramPerWriter, writerBufferSize, writerBufferSize / (threadsPerWriter + 2), threadsPerWriter)
      }
      buff.toList
    }
  }

  /**
    * Writes out additional schema entries to the file "additionalFields.list" in the SOLR config directory supplied.
    *
    * @param schemaFile
    * @param sourceConfDir
    */
  private def writeAdditionalSchemaEntries(schemaFile: File, sourceConfDir: File) = {
    if (DocBuilder.getAdditionalSchemaEntries.size() > 0) {

      logger.info("Writing " + DocBuilder.getAdditionalSchemaEntries.size() + " new fields into updated schema: " + schemaFile.getPath)
      val schemaString = FileUtils.readFileToString(schemaFile, "UTF-8")

      //remove 'bad' entries
      val sb = new StringBuilder()
      for (i: String <- DocBuilder.getAdditionalSchemaEntries.asScala) {
        if (!i.contains("name=\"\"") && !i.contains("name=\"_\")")) {
          sb.append(i)
          sb.append('\n')
        }
      }

      //export additional fields to a separate file
      FileUtils.writeStringToFile(new File(sourceConfDir + "/additionalFields.list"), sb.toString(), "UTF-8")
    } else {
      FileUtils.writeStringToFile(new File(sourceConfDir + "/additionalFields.list"), "", "UTF-8")
    }
  }

  /**
    * Performs the index merge and optimisation.
    *
    * @param solrHome
    * @param optimise
    * @param mergeSegments
    * @param schemaFile
    * @param dirs
    * @param mem
    */
  private def performMerge(solrHome: String, optimise: Boolean, mergeSegments: Int,
                           schemaFile: File, dirs: ArrayBuffer[String], mem: Int) = {
    if (mergeSegments > 0) {
      val segmentCount = mergeSegments
      val segmentSize = dirs.length / segmentCount + 1

      logger.info(s"Merging index into $segmentCount segments. source dirs=$dirs.length segment size=$segmentSize, mem=$mem mb")

      var dirsRemaining = dirs
      var segmentNumber = 0
      while (dirsRemaining.nonEmpty) {

        val (dirsSegment, remainder) = dirsRemaining.splitAt(Math.min(segmentSize, dirsRemaining.length))
        dirsRemaining = remainder

        logger.info(s"merged_$segmentNumber, $dirsSegment")

        IndexMergeTool.merge(
          solrHome + "merged_" + segmentNumber,
          dirsSegment.toArray,
          forceMerge = optimise,
          dirsSegment.length,
          deleteSources = false,
          mem
        )

        new File(solrHome + "merged_" + segmentNumber + "/conf").mkdirs()
        FileUtils.copyFile(schemaFile, new File(solrHome + "merged_" + segmentNumber + "/conf/schema.xml"))

        segmentNumber = segmentNumber + 1
      }
    }
  }

  /**
    * Setup the SOLR config to use, downloading from SOLR home if not available locally.
    *
    * @param solrHome
    * @param solrConfigXmlPath
    * @return
    */
  def setUpSolrConfig(solrHome: String, solrConfigXmlPath: String) : (IndexSchema, File, File, String, File) ={

    val confDir = solrHome + "/solr-create/biocache/conf"
    val newIndexDir = new File(confDir)
    if (newIndexDir.exists) {
      FileUtils.deleteDirectory(newIndexDir.getParentFile)
    }
    FileUtils.forceMkdir(newIndexDir)

    val sourceConfDir = new File(solrConfigXmlPath).getParentFile
    if(!sourceConfDir.exists()){
      //download from SOLR server if available
      FileUtils.forceMkdir(sourceConfDir)
      if(Config.solrHome.startsWith("http")) {
        Array("schema.xml", "additionalFields.list", "elevate.xml", "protwords.txt", "solrconfig.xml", "stopwords.txt", "synonyms.txt").foreach { fileName =>
          downloadFile(Config.solrHome + "/admin/file?file=" + fileName, sourceConfDir.getAbsolutePath + File.separator + fileName)
        }
      }
    }

    FileUtils.copyDirectory(sourceConfDir, newIndexDir)

    //identify the first valid schema
    val s1 = new File(confDir + "/schema.xml")
    val s2 = new File(confDir + "/schema.xml.bak")
    val s3 = new File(confDir + "/schema-managed")
    if (s3.exists()) {
      s3.delete()
    }
    val schemaFile: File = {
      if (s1.exists()) {
        s1
      } else {
        s2
      }
    }

    //with 6.6.2, this method moves the schema.xml to schema.xml.bak
    val schema = IndexSchemaFactory.buildIndexSchema(
      schemaFile.getName,
      SolrConfig.readFromResourceLoader(
        new SolrResourceLoader(new File(solrHome + "/solr-create/biocache").toPath),
        "solrconfig.xml"
      )
    )

    //recreate the /schema.xml
    if(!s1.exists() && s2.exists()) {
      FileUtils.copyFile(s2, s1)
    }

    FileUtils.writeStringToFile(new File(solrHome + "/solr-create/solr.xml"), "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><solr></solr>", "UTF-8")
    FileUtils.writeStringToFile(new File(solrHome + "/solr-create/zoo.cfg"), "", "UTF-8" )

    (schema, schemaFile, newIndexDir, confDir, sourceConfDir)
  }


  def downloadFile(url: String, fileToDownload: String)  {
    val src = scala.io.Source.fromURL(url)
    val out = new java.io.FileWriter(fileToDownload)
    out.write(src.mkString)
    out.close
  }
}