package au.org.ala.biocache

import java.io.File
import java.net.URI
import java.nio.file.Paths

import au.org.ala.biocache.index.IndexLocalNode
import org.apache.commons.io.FileUtils
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.store.FSDirectory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.slf4j.LoggerFactory

@RunWith(classOf[JUnitRunner])
class IndexingTest extends ConfigFunSuite {
  override val logger = LoggerFactory.getLogger("IndexingTest")

  test("Test range parsing"){

    val workingDir = System.getProperty("user.dir")
    val merged = new File(workingDir + "/src/test/resources/solr/biocache/merged_0")
    val confTmp = new File(workingDir + "/src/test/resources/solr/biocache/solr-create")

    if (merged.exists()) FileUtils.deleteDirectory(merged)
    if (confTmp.exists()) FileUtils.deleteDirectory(confTmp)

    // load some test data
    val pm = Config.persistenceManager

    (1 to 10000).foreach { idx =>
      val data = Map (
        "rowkey"           -> idx.toString,
        "scientificName"   -> "Macropus rufus",
        "scientificName_p" -> "Macropus rufus",
        "taxonConceptID_p" -> "urn:lsid:macropus:rufus"
      )
      pm.put(idx.toString, "occ", data, true, false)
    }

    val i = new IndexLocalNode
    i.indexRecords(1,
      workingDir + "/src/test/resources/solr/biocache/", //solrHome: String,
      workingDir + "/src/test/resources/solr/biocache/conf/solrconfig.xml",
      true, //optimise: Boolean,
      false, //optimiseOnly: Boolean,
      "", //checkpointFile: String,
      1, //threadsPerWriter: Int,
      1, //threadsPerProcess: Int,
      500, //ramPerWriter: Int,
      100, //writerSegmentSize: Int,
      1000, //processorBufferSize: Int,
      1000, //writerBufferSize: Int,
      100, //pageSize: Int,
      1, //mergeSegments: Int,
      false, //test: Boolean,
      1, //writerCount: Int,
      -1 //maxRecordsToIndex:Int = -1
    )

    //verify the index
    val indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(new URI("file://" + workingDir + "/src/test/resources/solr/biocache/merged_0"))))
    var counter = 0
    var lsidCount = 0
    var nameCount = 0

    val maxDocId = indexReader.maxDoc()

    while(counter < maxDocId) {
      val doc = indexReader.document(counter)
      lsidCount += doc.getValues("taxon_concept_lsid").size
      nameCount += doc.getValues("taxon_name").size
      counter += 1
    }
    indexReader.close

    //previous tests may have added data
    logger.info("LSID count = " + lsidCount)
    expectResult(true){lsidCount >= 10000}
    logger.info("Name count = " + nameCount)
    expectResult(true){nameCount >= 10000}

    if(merged.exists()) FileUtils.deleteDirectory(merged)
    if(confTmp.exists()) FileUtils.deleteDirectory(confTmp)
  }
}
