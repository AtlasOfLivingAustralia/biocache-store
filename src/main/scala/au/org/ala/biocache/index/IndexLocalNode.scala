package au.org.ala.biocache.index

import java.io.File

import au.org.ala.biocache._
import au.org.ala.biocache.index.lucene.{DocBuilder, LuceneIndexing}
import au.org.ala.biocache.util.Json
import org.apache.commons.io.FileUtils
import org.apache.solr.core.{SolrConfig, SolrResourceLoader}
import org.apache.solr.schema.{IndexSchema, IndexSchemaFactory}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
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
                   maxRecordsToIndex:Int = -1
                  ) : Long = {

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

    counter.counter.get()
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
    if (!DocBuilder.getAdditionalSchemaEntries.isEmpty) {

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

      logger.info(s"Merging index from $segmentCount segments. source dirs=$dirs.length segment size=$segmentSize, mem=$mem mb")

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
        // update SOLR schema with any newly sampled fields
        val solrIndexUpdate = new SolrIndexDAO(Config.solrHome, Config.excludeSensitiveValuesFor, Config.extraMiscFields)
        solrIndexUpdate.addLayerFieldsToSchema()

        downloadDirectory(Config.solrHome, null, sourceConfDir.getPath, false)

        // SOLR will internally import schema.xml and export it to managed-schema when managed-schema is absent.
        // Dynamically added SOLR fields are only added to managed-schema.
        // IndexLocalNode will only use schema.xml.

        val schemaFile = new File(sourceConfDir.getAbsolutePath + "/schema.xml")
        val schemaFileBak = new File(sourceConfDir.getAbsolutePath + "/schema.xml.bak")
        val schemaManaged = new File(sourceConfDir.getAbsolutePath + "/managed-schema")

        // Use the most up to date schema (manage-schema, else schema.xml, else schema.xml.bak)
        if (schemaManaged.exists()) FileUtils.copyFile(schemaManaged, schemaFile)
        if (!schemaFile.exists() && schemaFileBak.exists()) FileUtils.copyFile(schemaFileBak, schemaFile)

        // Delete unused schema files
        if (schemaManaged.exists()) schemaManaged.delete()
        if (schemaFileBak.exists()) schemaFileBak.delete()

        if (!schemaFile.exists()) {
          throw new RuntimeException("Unable to find a schema.xml to use for indexing.")
        }
      }
    }

    FileUtils.copyDirectory(sourceConfDir, newIndexDir)

    val schemaFile = new File(confDir + "/schema.xml")
    val schemaFileBak = new File(confDir + "/schema.xml.bak")

    //with 6.6.2, this method moves the schema.xml to schema.xml.bak
    val schema = IndexSchemaFactory.buildIndexSchema(
      schemaFile.getName,
      SolrConfig.readFromResourceLoader(
        new SolrResourceLoader(new File(solrHome + "/solr-create/biocache").toPath),
        "solrconfig.xml"
      )
    )

    //recreate the /schema.xml
    if (!schemaFile.exists() && schemaFileBak.exists()) {
      FileUtils.moveFile(schemaFileBak, schemaFile)
    }

    FileUtils.writeStringToFile(new File(solrHome + "/solr-create/solr.xml"), "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><solr></solr>", "UTF-8")
    FileUtils.writeStringToFile(new File(solrHome + "/solr-create/zoo.cfg"), "", "UTF-8" )

    (schema, schemaFile, newIndexDir, confDir, sourceConfDir)
  }

  def downloadDirectory(solrHome: String, path: String, outputPath: String, failSilently: Boolean): Unit = {
    val url = if (path == null) {
      solrHome + "/admin/file?wt=json"
    } else {
      solrHome + "/admin/file?wt=json&file=" + path
    }

    try {
      val src = scala.io.Source.fromURL(url)
      val response = Json.toMap(src.mkString)

      response.get("files").get.asInstanceOf[java.util.Map[String, java.util.Map[String, Object]]].foreach(file => {

        val fileOutputPath = outputPath + "/" + file._1

        val newPath = if (path != null) {
          path + "\\" + file._1
        } else {
          file._1
        }
        if ("true".equals(file._2.getOrElse("directory", "false").toString)) {
          FileUtils.forceMkdir(new File(fileOutputPath + "/" + newPath))
          downloadDirectory(solrHome, newPath, fileOutputPath, failSilently)
        } else {
          downloadFile(solrHome + "/admin/file?file=" + newPath, fileOutputPath, failSilently)
        }


      })

    } catch {
      case e: Exception => {
        if (!failSilently) throw e
      }
    }
  }

  def downloadFile(url: String, fileToDownload: String, failSilently:Boolean)  {
    try {
      val src = scala.io.Source.fromURL(url)
      val out = new java.io.FileWriter(fileToDownload)
      out.write(src.mkString)
      out.close
    } catch {
      case e:Exception => {
        if(!failSilently) throw e
      }
    }
  }
}