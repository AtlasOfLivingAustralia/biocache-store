package au.org.ala.biocache.index

import java.io._

import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.util.OptionParser
import org.apache.commons.io.FileUtils
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{ConcurrentMergeScheduler, IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.FSDirectory
import org.slf4j.LoggerFactory

/**
  * Thin wrapper around SOLR index merge tool that allows it to be incorporated
  * into the CMD2.
  *
  * TODO add support for directory patterns e.g. /data/solr-create/biocache-thread-{wildcard}/data/index
  */
object IndexMergeTool extends Tool {

  def cmd = "index-merge"

  def desc = "Merge indexes "

  val logger = LoggerFactory.getLogger("IndexMergeTool")

  def main(args: Array[String]) {

    var mergeDir = ""
    var directoriesToMerge = Array[String]()
    var forceMerge = true
    var mergeSegments = 1
    var deleteSources = false
    var ramBuffer = 4096.0d

    val parser = new OptionParser(help) {
      arg("<merge-dr>", "The output path for the merged index", {
        v: String => mergeDir = v
      })
      arg("<to-merge>", "Pipe separated list of directories to merge", {
        v: String => directoriesToMerge = v.split('|').map(x => x.trim)
      })
      opt("sk", "skipMerge", "Skip merge of segments.", {
        forceMerge = false
      })
      intOpt("ms", "max-segments", "Max merge segments. Default " + mergeSegments, {
        v: Int => mergeSegments = v
      })
      doubleOpt("ram", "ram-buffer", "RAM buffer size. Default " + ramBuffer, {
        v: Double => ramBuffer = v
      })
      opt("ds", "delete-sources", "Delete sources if successful. Defaults to " + deleteSources, {
        deleteSources = true
      })
    }
    if (parser.parse(args)) {
      merge(mergeDir, directoriesToMerge, forceMerge, mergeSegments, deleteSources, ramBuffer)
    }
  }

  /**
    * Merge method that wraps SOLR merge API
    *
    * @param mergeDir
    * @param directoriesToMerge
    * @param forceMerge
    * @param mergeSegments
    */
  def merge(mergeDir: String, directoriesToMerge: Array[String], forceMerge: Boolean, mergeSegments: Int, deleteSources: Boolean, rambuffer: Double = 4096.0d) {
    val start = System.currentTimeMillis()

    logger.info("Merging to directory:  " + mergeDir)
    directoriesToMerge.foreach(x => println("Directory included in merge: " + x))

    val mergeDirFile = new File(mergeDir)
    if (mergeDirFile.exists()) {
      //clean out the directory
      mergeDirFile.listFiles().foreach(f => FileUtils.forceDelete(f))
    } else {
      FileUtils.forceMkdir(mergeDirFile)
    }

    val mergedIndex = FSDirectory.open(mergeDirFile.toPath)

    val writerConfig = new IndexWriterConfig(null)
      .setOpenMode(OpenMode.CREATE)
      .setRAMBufferSizeMB(rambuffer)

    writerConfig.setMergeScheduler(new ConcurrentMergeScheduler)
    writerConfig.getMergeScheduler.asInstanceOf[ConcurrentMergeScheduler].setMaxMergesAndThreads(mergeSegments, mergeSegments)

    val writer = new IndexWriter(mergedIndex, writerConfig)
    val indexes = directoriesToMerge.map(dir => FSDirectory.open(new File(dir).toPath))

    logger.info("Adding indexes...")
    writer.addIndexes(indexes: _*)

    if (forceMerge) {
      logger.info("Full merge...")
      writer.forceMerge(mergeSegments)
    } else {
      logger.info("Skipping merge...")
    }

    writer.close()
    val finish = System.currentTimeMillis()
    logger.info("Merge complete:  " + mergeDir + ". Time taken: " + ((finish - start) / 1000) / 60 + " minutes")

    if (deleteSources) {
      logger.info("Deleting source directories")
      directoriesToMerge.foreach(dir => FileUtils.forceDelete(new File(dir)))
      logger.info("Deleted source directories")
    }
  }
}
