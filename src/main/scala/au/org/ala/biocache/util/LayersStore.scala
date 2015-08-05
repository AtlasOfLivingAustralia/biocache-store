package au.org.ala.biocache.util

import java.util

import net.sf.json.JSONArray
import org.ala.layers.dao.IntersectCallback
import org.ala.layers.dto.IntersectionFile
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.HttpGet
import scala.io.Source
import java.io.{BufferedInputStream, InputStreamReader}
import org.slf4j.LoggerFactory

class LayersStore ( layersStoreUrl: String) {

  val logger = LoggerFactory.getLogger("LayersStore")

  /**
   * Sampling
   * - strings contains valid fieldIds, see getFieldIds()
   * - doubles is Array(Array(longitude,latitude))
   * returns the sampling csv as a Reader e.g. csv = new CSVReader(sample(strings, doubles))
   */
  def sample(strings: Array[String], doubles: Array[Array[Double]], callback: IntersectCallback = null): java.io.Reader = {

    //format inputs
    val doublesString: StringBuilder = new StringBuilder()
    for ( i <- 0 until doubles.length ) {
      if ( i > 0) {
        doublesString.append(",")
      }
      doublesString.append(doubles(i)(1)).append(",").append(doubles(i)(0))
    }
    val stringsString: StringBuilder = new StringBuilder()
    for ( i <- 0 until strings.length ) {
      if ( i > 0) {
        stringsString.append(",")
      }
      stringsString.append(strings(i))
    }

    //start
    val statusUrl = samplingStart(stringsString.toString(), doublesString.toString())

    //monitor (sleep 3s if > 1000 points, else 1s)
    val sleepLength = if (doubles.length < 1000) 1000 else 3000
    var (respCode, respBody, retry) = samplingStatus(statusUrl)

    while( retry ) {
      Thread.sleep(sleepLength)
      val r = samplingStatus(statusUrl)
      respCode = r._1
      respBody = r._2
      retry = r._3

      if (callback != null) {
        val json = Json.toMap(respBody)
        if (json.get("status").get == "waiting") {
          callback.progressMessage("In queue for sampling.")
        } else if (json.get("status").get == "error") {
          callback.progressMessage("Error while sampling.")
        } else if (json.get("status").get == "finished") {
          callback.setCurrentLayerIdx(strings.length - 1)
          callback.setCurrentLayer(new IntersectionFile("","","","finished. Now loading", "","","","",null))
          callback.progressMessage("Loading sampling.")
        } else if (json.get("status").get == "started") {
          try {
            var pos: Integer = Integer.parseInt(String.valueOf(json.get("progress").get))
            callback.setCurrentLayerIdx(pos)
            callback.setCurrentLayer(new IntersectionFile("","","","layer " + (pos + 1), "","","","",null))
          } catch {
            case _: Exception => {
              logger.error("Failed to check progress: " + respBody)
              (null)
            }
          }
          callback.progressMessage("Sampling " + json.get("progress") + " of " + json.get("fields") + " layers.")
        }
      }
    }

    try {
      if (callback != null) {
        callback.setCurrentLayerIdx(strings.length - 1)
        callback.setCurrentLayer(new IntersectionFile("","","","finished. Now loading", "","","","",null))
        callback.progressMessage("Loading sampling.")
      }

      val downloadUrl = Json.toMap(respBody).get("downloadUrl").get.asInstanceOf[String]

      //download stream as csv
      val inputStream: BufferedInputStream = new BufferedInputStream(new java.net.URL(downloadUrl + "?csv=true").openStream())
      (new InputStreamReader(inputStream))

    } catch {
      case _:Exception => {
        logger.error("Failed to download from download_url: " + respBody)
        (null)
      }
    }
  }

  /*
  returns list of valid fieldIds for sampling
   */
  def getFieldIds() : util.ArrayList[String] = {
    val httpClient = new DefaultHttpClient()
    val httpGet = new HttpGet(layersStoreUrl + "/fieldsdb")
    val response = httpClient.execute(httpGet)
    val result = response.getStatusLine()
    val responseBody: String = Source.fromInputStream(response.getEntity().getContent()).mkString
    logger.debug("Response code: " + result.getStatusCode)

    val fields: util.ArrayList[String] = new util.ArrayList[String]()
    val ja: JSONArray = JSONArray.fromObject(responseBody)
    for (j <- 0 until ja.size()) {
      fields.add(ja.getJSONObject(j).getString("id"))
    }

    (fields)
  }

  /**
   * Returns map of valid fieldIds for sampling
   * key = fieldId
   * value = display name
   */
  def getFieldIdsAndDisplayNames() : util.HashMap[String, String] = {

    val response = Source.fromURL(layersStoreUrl + "/fieldsdb", "UTF-8")
    val responseBody = if(!response.isEmpty){
      response.mkString
    } else {
      "[]"
    }

    val fields: util.HashMap[String, String] = new util.HashMap[String, String]()
    val ja: JSONArray = JSONArray.fromObject(responseBody)
    for (j <- 0 until ja.size()) {
      fields.put(ja.getJSONObject(j).getString("id"), ja.getJSONObject(j).getString("name"))
    }

    (fields)
  }

  private def samplingStart(fields:String, points:String) : (String) = {
    val (responseCode, respBody) = HttpUtil.postBody(layersStoreUrl + "/intersect/batch", "application/json", "fids=" + fields + "&points=" + points)

    (Json.toMap(respBody).get("statusUrl").get.asInstanceOf[String])
  }

  private def samplingStatus(statusUrl: String) : (Int, String, Boolean) = {
    val httpClient = new DefaultHttpClient()
    val httpGet = new HttpGet(statusUrl)
    val response = httpClient.execute(httpGet)
    val result = response.getStatusLine()
    val responseBody = Source.fromInputStream(response.getEntity().getContent()).mkString
    logger.debug("Response code: " + result.getStatusCode)
    val json = Json.toMap(responseBody)

    val status  = json.get("status").get
    if (status.equals("error") || status.equals("cancelled")) {
      (result.getStatusCode, responseBody, false)
    } else if (status.equals("finished")) {
      (result.getStatusCode, responseBody, false)
    } else {
      (result.getStatusCode, responseBody, true)
    }
  }
}
