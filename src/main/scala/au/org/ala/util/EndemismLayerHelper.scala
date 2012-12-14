package au.org.ala.util

import collection.mutable.ListBuffer
import java.text.MessageFormat
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import au.org.ala.biocache.Config
import org.apache.commons.codec.net.URLCodec
import org.apache.commons.math3.util.Precision
import org.apache.commons.io.IOUtils
import java.io.{BufferedWriter, FileWriter}
import org.apache.commons.lang3.StringUtils

/**
 * Created with IntelliJ IDEA.
 * User: ChrisF
 * Date: 29/11/12
 * Time: 4:13 PM
 * To change this template use File | Settings | File Templates.
 */
object EndemismLayerHelper {
  val FACET_DOWNLOAD_URL_TEMPLATE = Config.biocacheServiceUrl + "/occurrences/facets/download?q={0}&facets={1}"
  //val FACET_DOWNLOAD_URL_TEMPLATE = "http://ala-rufus.it.csiro.au/biocache-service/occurrences/facets/download?q={0}&facets={1}"

  val ALL_SPECIES_QUERY = "species_guid:[* TO *] AND geospatial_kosher:true"
  val SPECIES_QUERY_TEMPLATE = "species_guid:{0} AND geospatial_kosher:true"

  val SPECIES_FACET = "species_guid"
  val POINT_001_FACET = "point-0.001"

  def main(args: Array[String]) {
    val helper = new EndemismLayerHelper();
    var allSpecies = false;
    var speciesCellCountsFilePath: String = null;
    var cellSpeciesFilePath: String = null;

    val parser = new OptionParser("Find expert distribution outliers") {
      arg("speciesCellCountsFilePath", "File to write species cell counts to", {
        v: String => speciesCellCountsFilePath = v
      })
      arg("cellSpeciesFilePath", "File to write cell species lists to", {
        v: String => cellSpeciesFilePath = v
      })
      booleanOpt("a", "allSpecies", "If true, endemism values will be calcuated for all species, instead of those that were recently updated.", {
        v: Boolean => allSpecies = v
      })
    }

    if (parser.parse(args)) {
      helper.calculateSpeciesEndemismValues(speciesCellCountsFilePath, cellSpeciesFilePath, allSpecies)
    }
  }
}

class EndemismLayerHelper {

  val indexDAO = Config.indexDAO

  def doThing(s : String, i : Int) : Boolean = {
    println(s + ": " + i)
    true
  }

  def calculateSpeciesEndemismValues(speciesCellCountsFilePath: String, cellSpeciesFilePath: String, allSpecies: Boolean) {
    var cellSpecies = Map[String, Set[String]]()
    var speciesCellCounts = Map[String, Int]()

    // get list of species
    //val speciesLsids = doFacetDownload(EndemismLayerHelper.ALL_SPECIES_QUERY, EndemismLayerHelper.SPECIES_FACET)
    indexDAO.pageOverFacet(doThing, "species_guid", "geospatial_kosher:true", Array())

    /*
    // remove first line as this will contain the text "taxon_concept_id"
    speciesLsids.remove(0)

    for (lsid <- speciesLsids) {
      val occurrencePoints = doFacetDownload(MessageFormat.format(EndemismLayerHelper.SPECIES_QUERY_TEMPLATE, lsid), EndemismLayerHelper.POINT_001_FACET)

      // remove first line as this will contain the text "taxon_concept_id"
      occurrencePoints.remove(0)

      var pointsSet = Set[String]()

      var minLatitude = Double.PositiveInfinity
      var maxLatitude = Double.NegativeInfinity
      var minLongitude = Double.PositiveInfinity
      var maxLongitude = Double.NegativeInfinity

      var plus360MinLongitude = Double.PositiveInfinity
      var plus360MaxLongitude = Double.NegativeInfinity

      for (point <- occurrencePoints) {
        val splitPoint = point.split(",")
        val strLatitude = splitPoint(0)
        val strLongitude = splitPoint(1)

        val roundedLatitude = Precision.round(java.lang.Double.parseDouble(strLatitude), 2, java.math.BigDecimal.ROUND_CEILING)
        val roundedLongitude = Precision.round(java.lang.Double.parseDouble(strLongitude), 2, java.math.BigDecimal.ROUND_FLOOR)

        val strRoundedCoords = roundedLatitude + "," + roundedLongitude
        pointsSet += strRoundedCoords

        var thisCellSpecies = cellSpecies.getOrElse(strRoundedCoords, null)
        if (thisCellSpecies == null) {
          thisCellSpecies = Set[String]()
        }

        thisCellSpecies += lsid
        cellSpecies += (strRoundedCoords -> thisCellSpecies)
      }

      speciesCellCounts += (lsid -> pointsSet.size)
      println(lsid + " - " + pointsSet.size + " cells")
    }

    val bwSpeciesCellCounts = new BufferedWriter(new FileWriter(speciesCellCountsFilePath));
    for (lsid <- speciesCellCounts.keys) {
      bwSpeciesCellCounts.append(lsid)
      bwSpeciesCellCounts.append(",")
      bwSpeciesCellCounts.append(speciesCellCounts(lsid).toString)
      bwSpeciesCellCounts.newLine()
    }
    bwSpeciesCellCounts.flush()
    bwSpeciesCellCounts.close()

    val bwCellSpecies = new BufferedWriter(new FileWriter(cellSpeciesFilePath));
    for (cellCoords <- cellSpecies.keys) {
      bwCellSpecies.append(cellCoords)
      bwCellSpecies.append(",")
      bwCellSpecies.append(cellSpecies(cellCoords).mkString(","))
      bwCellSpecies.newLine()
    }
    bwCellSpecies.flush()
    bwCellSpecies.close()
    */
  }

//  def doFacetDownload(query: String, facet: String): ListBuffer[String] = {
//    val urlCodec = new URLCodec()
//
//    val url = MessageFormat.format(EndemismLayerHelper.FACET_DOWNLOAD_URL_TEMPLATE, urlCodec.encode(query), urlCodec.encode(facet))
//
//    val httpClient = new HttpClient()
//    val get = new GetMethod(url)
//    try {
//      val responseCode = httpClient.executeMethod(get)
//      if (responseCode == 200) {
//        val contentStream = get.getResponseBodyAsStream();
//
//        val lines = new ListBuffer[String]
//
//        for (line <- IOUtils.readLines(contentStream).toArray()) {
//          lines += line.asInstanceOf[String]
//        }
//
//        contentStream.close()
//
//        return lines;
//      } else {
//        throw new Exception("facet download request failed (" + responseCode + ")")
//      }
//    } finally {
//      get.releaseConnection()
//      httpClient.getHttpConnectionManager.closeIdleConnections(0)
//    }
//  }

}