package au.org.ala.biocache.index

import java.io.File

import au.org.ala.biocache._
import au.org.ala.biocache.caches.TaxonProfileDAO
import au.org.ala.biocache.index.lucene.{DocBuilder, LuceneIndexing}
import au.org.ala.biocache.persistence.Cassandra3PersistenceManager
import org.apache.commons.io.FileUtils
import org.apache.solr.core.{SolrConfig, SolrResourceLoader}
import org.apache.solr.schema.IndexSchemaFactory
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * A class that can be used to reload taxon conservation values for all records.
  *
  * @param centralCounter
  * @param threadId
  */
class LoadTaxonConservationData(centralCounter: Counter, threadId: Int) extends Runnable {

  val logger = LoggerFactory.getLogger("LoadTaxonConservationData")
  var ids = 0
  val threads = 2
  var batches = 0

  def run {
    var counter = 0
    val start = System.currentTimeMillis
    val sep = Config.persistenceManager.fieldDelimiter

    val batch: mutable.Map[String, Map[String, String]] = mutable.Map[String, Map[String, String]]()

    Config.persistenceManager.asInstanceOf[Cassandra3PersistenceManager].pageOverLocal("occ", (guid, map, _) => {
      val updates = mutable.Map[String, String]()
      val taxonProfileWithOption = TaxonProfileDAO.getByGuid(map.getOrElse("taxonConceptID" + sep + "p", ""))
      if (!taxonProfileWithOption.isEmpty) {
        val taxonProfile = taxonProfileWithOption.get
        //add the conservation status if necessary
        if (taxonProfile.conservation != null) {
          val country = taxonProfile.retrieveConservationStatus(map.getOrElse("country" + sep + "p", ""))
          updates.put("countryConservation" + sep + "p", country.getOrElse(""))
          val state = taxonProfile.retrieveConservationStatus(map.getOrElse("stateProvince" + sep + "p", ""))
          updates.put("stateConservation" + sep + "p", state.getOrElse(""))
          val global = taxonProfile.retrieveConservationStatus("global")
          updates.put("global", global.getOrElse(""))
        }
      }
      if (updates.size < 3) {
        updates.put("countryConservation" + sep + "p", "")
        updates.put("stateConservation" + sep + "p", "")
        updates.put("global", "")
      }
      val changes = updates.filter(it => map.getOrElse(it._1, "") != it._2)
      if (!changes.isEmpty) {
        batch.put(guid, updates.toMap)
      }

      counter += 1
      if (counter % 10000 == 0) {
        logger.info("[LoadTaxonConservationData Thread " + threadId + "] Import of sample data " + counter + " Last key " + guid)

        if (!batch.isEmpty) {
          logger.info("writing")
          Config.persistenceManager.putBatch("occ", batch.toMap, true, false)
          batch.clear()
        }
      }

      true
    }, 4, Array("taxonConceptID" + sep + "p", "country" + sep + "p", "countryConservation" + sep + "p", "stateProvince" + sep + "p", "stateConservation" + sep + "p", "global"))

    if (!batch.isEmpty) {
      logger.info("writing")
      Config.persistenceManager.putBatch("occ", batch.toMap, true, false)
      batch.clear()
    }

    val fin = System.currentTimeMillis
    logger.info("[LoadTaxonConservationData Thread " + threadId + "] " + counter + " took " + ((fin - start).toFloat) / 1000f + " seconds")
    logger.info("Finished.")
  }
}

/**
  * A class for local records indexing.
  */
class IndexLocalRecordsV2 {

  val logger: Logger = LoggerFactory.getLogger("IndexLocalRecordsV2")

  def indexRecords(numThreads: Int, solrHome: String, solrConfigXmlPath: String, optimise: Boolean, optimiseOnly: Boolean,
                   checkpointFile: String, threadsPerWriter: Int, threadsPerProcess: Int, ramPerWriter: Int,
                   writerSegmentSize: Int, processorBufferSize: Int, writerBufferSize: Int,
                   pageSize: Int, mergeSegments: Int, test: Boolean, writerCount: Int, testMap: Boolean
                  ): Unit = {

    val start = System.currentTimeMillis()

    System.setProperty("tokenRangeCheckPointFile", checkpointFile)

    var count = 0
    val singleWriter = writerCount == 0

    ///init for luceneIndexing
    var luceneIndexing: ArrayBuffer[LuceneIndexing] = new ArrayBuffer[LuceneIndexing]

    val confDir = solrHome + "/solr-create/biocache/conf"
    //solr-create/thread-0/conf
    val newIndexDir = new File(confDir)
    if (newIndexDir.exists) {
      FileUtils.deleteDirectory(newIndexDir.getParentFile)
    }
    FileUtils.forceMkdir(newIndexDir)

    //CREATE a copy of SOLR home
    val sourceConfDir = new File(solrConfigXmlPath).getParentFile
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
    val schema = IndexSchemaFactory.buildIndexSchema(schemaFile.getName,
      SolrConfig.readFromResourceLoader(new SolrResourceLoader(new File(solrHome + "/solr-create/biocache").toPath), "solrconfig.xml"))

    FileUtils.writeStringToFile(new File(solrHome + "/solr-create/solr.xml"), "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><solr></solr>", "UTF-8")
    FileUtils.writeStringToFile(new File(solrHome + "/solr-create/zoo.cfg"), "", "UTF-8" )

    if (singleWriter) {
      val outputDir = newIndexDir.getParent + "/data0-"
      logger.info("Writing index to " + outputDir)
      luceneIndexing += new LuceneIndexing(schema, writerSegmentSize.toLong, outputDir,
        ramPerWriter, writerBufferSize, writerBufferSize / 2, threadsPerWriter)
    } else {
      for (i <- 0 until writerCount) {
        val outputDir = newIndexDir.getParent + "/data" + i + "-"
        logger.info("Writing index to " + outputDir)
        luceneIndexing += new LuceneIndexing(schema, writerSegmentSize.toLong, outputDir,
          ramPerWriter, writerBufferSize, writerBufferSize / (threadsPerWriter + 2), threadsPerWriter)
      }
    }

    if (test) {
      DocBuilder.setIsIndexing(false)
    }

    val counter: Counter = new DefaultCounter()
    if (testMap) {
      new IndexRunnerMap(counter,
        confDir,
        pageSize,
        luceneIndexing,
        threadsPerProcess,
        processorBufferSize,
        singleWriter, test, numThreads
      ).run()
    } else {
      new IndexRunner(counter,
        confDir,
        pageSize,
        luceneIndexing,
        threadsPerProcess,
        processorBufferSize,
        singleWriter, test, numThreads
      ).run()
    }

    val end = System.currentTimeMillis()
    logger.info("Indexing completed. Total indexed : " + counter.counter + " in " + ((end - start).toFloat / 1000f / 60f) + " minutes")

    val dirs = new ArrayBuffer[String]()

    if (singleWriter) {
      luceneIndexing(0).close(true, false)
    }
    for (i <- luceneIndexing.indices) {
      for (j <- 0 until luceneIndexing(i).getOutputDirectories.size()) {
        dirs += luceneIndexing(i).getOutputDirectories.get(j).getPath
      }
    }

    luceneIndexing = null
    System.gc()

    val mem = Math.max((Runtime.getRuntime.freeMemory() * 0.75) / 1024 / 1024, writerCount * ramPerWriter).toInt

    //insert new fields into the schema file 's1'
    val s: File = {
      if (s1.exists()) {
        s1
      } else {
        s2
      }
    }
    if (DocBuilder.getAdditionalSchemaEntries.size() > 0) {
      logger.info("Writing " + DocBuilder.getAdditionalSchemaEntries.size() + " new fields into updated schema: " + s1.getPath)
      val schemaString = FileUtils.readFileToString(s)
      //      FileUtils.writeStringToFile(s1, schemaString.replace("</schema>",
      //        StringUtils.join(DocBuilder.getAdditionalSchemaEntries, "\n") + "\n</schema>"))

      //backup and overwrite source schema
      //      FileUtils.copyFile(new File(sourceConfDir + "/schema.xml"),
      //        new File(sourceConfDir + "/schema.xml." + System.currentTimeMillis()))
      //      FileUtils.copyFile(s1, new File(sourceConfDir + "/schema.xml"))

      //remove 'bad' entries
      val sb = new StringBuilder()
      for (i: String <- DocBuilder.getAdditionalSchemaEntries.asScala) {
        if (!i.contains("name=\"\"") && !i.contains("name=\"_\")")) {
          sb.append(i)
          sb.append('\n')
        }
      }

      //export additional fields to a separate file
      FileUtils.writeStringToFile(new File(sourceConfDir + "/additionalFields.list"),
        sb.toString())
    } else {
      FileUtils.writeStringToFile(new File(sourceConfDir + "/additionalFields.list"), "")
    }

    if (mergeSegments > 0) {
      val segmentCount = mergeSegments
      val segmentSize = dirs.length / segmentCount + 1

      logger.info("Merging index into " + segmentCount + " segments. source dirs=" + dirs.length + ", segment size=" + segmentSize + ", mem=" + mem + "mb")

      var dirsRemaining = dirs
      var segmentNumber = 0
      while (dirsRemaining.nonEmpty) {
        var (dirsSegment, remainder) = dirsRemaining.splitAt(Math.min(segmentSize, dirsRemaining.length))
        dirsRemaining = remainder

        logger.info("merged_" + segmentNumber + ", " + dirsSegment)

        IndexMergeTool.merge(solrHome + "merged_" + segmentNumber, dirsSegment.toArray, forceMerge = optimise, dirsSegment.length, deleteSources = false, mem)

        new File(solrHome + "merged_" + segmentNumber + "/conf").mkdirs()
        FileUtils.copyFile(s, new File(solrHome + "merged_" + segmentNumber + "/conf/schema.xml"))

        segmentNumber = segmentNumber + 1
      }
    }

    logger.info("Complete")

    //Move checkpoint file if complete
    new File(checkpointFile).renameTo(new File(checkpointFile + ".complete"))
  }
}

