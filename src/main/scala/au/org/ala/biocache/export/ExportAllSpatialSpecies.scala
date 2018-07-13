package au.org.ala.biocache.export

import java.io.{File, FileWriter}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ExecutorService, Executors}

import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.util.{FileHelper, OptionParser}
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport

/**
  * A utility that exports data from the search indexes for offline processes.
  *
  * Uses one streamer to write the spatial species to multiple files based on a number of
  * "threads" that will be used to consume the files.
  */
object ExportAllSpatialSpecies extends Tool {

  val logger = LoggerFactory.getLogger("ExportAllSpatialSpecies")

  def cmd = "export-by-species"

  def desc = "Export by species CSV data used by outlier, duplicate detection. From SOLR"

  def main(args: Array[String]) {

    var threads = 4
    var exportDirectory = "/data/offline/exports"
    var lastWeek = false

    val parser = new OptionParser(help) {
      arg("output-directory", "the output directory for the exports", {
        v: String => exportDirectory = v
      })
      intOpt("t", "threads", "the number of threads/files to have for the exports", {
        v: Int => threads = v
      })
      opt("lastWeek", "species that have changed in the last week", {
        lastWeek = true
      })
    }

    if (parser.parse(args)) {
      new ExportAllSpatialSpecies().export(lastWeek, threads, exportDirectory)
    }
  }
}

/**
  * A utility for exporting spatial data.
  */
class ExportAllSpatialSpecies {

  val logger = LoggerFactory.getLogger("ExportAllSpatialSpecies")

  //Warning changing these fields may cause issues in the offline processing tasks
  //TODO - make environmental properties configurable
  val fieldsToExport = Array(
    "id",
    "species_guid",
    "subspecies_guid",
    "year",
    "month",
    "occurrence_date",
    "point-1",
    "point-0.1",
    "point-0.01",
    "point-0.001",
    "point-0.0001",
    "lat_long",
    "raw_taxon_name",
    "collectors",
    "duplicate_status",
    "duplicate_record",
    "latitude",
    "longitude",
    "el882",
    "el889",
    "el887",
    "el865",
    "el894",
    "coordinate_uncertainty",
    "record_number",
    "catalogue_number"
  )

  val query = "lat_long:*"
  val filterQueries = Array[String]()
  //val sortFields = Array("species_guid", "subspecies_guid", "row_key")
  val sortFields = Array()
  val multivaluedFields = Some(Array("duplicate_record"))

  import FileHelper._

  def export(lastWeek: Boolean, threads: Int, exportDirectory: String): Unit = {

    var validGuids: Option[List[String]] = None

    if (lastWeek) {
      //need to obtain a list of species guids that have changed in the last week
      def filename = exportDirectory + File.separator + "delta-species-guids.txt"

      logger.info("Export GUIDs of species that have changed in last week to: " + filename)
      val args = Array("species_guid", filename, "--lastWeek", "true", "--open")
      ExportFacet.main(args)
      //now load the acceptable lsids into the list
      val buf = new ArrayBuffer[String]()
      new File(filename).foreachLine { line => buf += line }
      validGuids = Some(buf.toList)
      logger.info("There are " + buf.size + " valid guids to download")
    }

    var ids = 0
    //construct all the file writers that will be randomly assigned taxon concepts
    val files: Array[(FileWriter, FileWriter)] = Array.fill(threads) {
      val file = new File(exportDirectory + File.separator + ids)
      FileUtils.forceMkdir(file)
      ids += 1
      val speciesOutFile = file.getAbsolutePath + File.separator + "species.out"
      val subSpeciesOutFile = file.getAbsolutePath + File.separator + "subspecies.out"
      logger.info(s"Exporting to: $speciesOutFile, and $subSpeciesOutFile")
      (new FileWriter(new File(speciesOutFile)), new FileWriter(new File(subSpeciesOutFile)))
    }

    var counter = 0
    var lsidCount = new AtomicInteger(0)

    //get list of taxonIDs and query by taxonID
    logger.info(s"getting species guids")
    val speciesGuid = Config.indexDAO.getDistinctValues("*:*", "species_guid",10000000)

    logger.info(s"getting species guids  = "  + speciesGuid.get.size)
    logger.info(s"getting subspecies guids")
    val subspeciesGuid = Config.indexDAO.getDistinctValues("*:*", "subspecies_guid",10000000)

    logger.info(s"getting subspecies guids  = "  + subspeciesGuid.get.size)
    val guidSet = (speciesGuid.get ++ subspeciesGuid.get)

    guidSet.par.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(threads))
    guidSet.par.foreach { guid =>

      val lsidCountInt = lsidCount.incrementAndGet()
      logger.info("Starting to export of:  " + guid)

      //allocate the writer
      val (speciesWriter, subspeciesWriter) = files(lsidCountInt % threads)

      Config.indexDAO.streamIndex(map => {

        val outputLine = fieldsToExport.map { f => getFromMap(map, f) }.mkString("\t")
        counter += 1

        speciesWriter.write(outputLine)
        speciesWriter.write("\n")

        val subspecies = map.get("subspecies_guid")
        if (subspecies != null) {
          subspeciesWriter.write(outputLine)
          subspeciesWriter.write("\n")
        }

        if (counter % 10000 == 0) {
          speciesWriter.flush
          subspeciesWriter.flush
        }

        true
      }, fieldsToExport, s"""species_guid:"$guid"""", Array(query), Array(), multivaluedFields)
    }

    files.foreach {
      case (fw1, fw2) => {
        fw1.flush()
        fw1.close()
        fw2.flush()
        fw2.close()
      }
    }
    logger.info("Export finished")
  }

  private def getFromMap(map: java.util.Map[String, AnyRef], key: String): String = {
    val value = map.get(key)
    if (value == null) {
      ""
    } else {
      value.toString.replaceAll("(\r\n|\n)", " ")
    }
  }
}
