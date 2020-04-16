package au.org.ala.biocache.util

import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.entity.mime.{HttpMultipartMode, MultipartEntity}
import org.apache.http.entity.mime.content.{FileBody, StringBody}
import org.apache.http.client.methods.HttpPost

import scala.io.Source
import java.io.File
import java.net.URL

import org.apache.http.entity.StringEntity
import org.slf4j.LoggerFactory

object HttpUtil {

  val logger = LoggerFactory.getLogger("HttpUtil")

  def get(url: String): (String) = Source.fromURL(new URL(url)).mkString

  def postBody(url: String, contentType: String, stringBody: String): (Int, String) = {
    val httpClient = new DefaultHttpClient()
    try {
      val httpPost = new HttpPost(url)
      val stringEntity = new StringEntity(stringBody)
      stringEntity.setContentType(contentType)
      stringEntity.setContentEncoding("UTF-8")
      httpPost.setEntity(new StringEntity(stringBody, "UTF-8"))
      val response = httpClient.execute(httpPost)
      try {
        val result = response.getStatusLine()
        val responseBody = Source.fromInputStream(response.getEntity().getContent()).mkString
        if (logger.isDebugEnabled()) {
          logger.debug("Response code: " + result.getStatusCode)
        }
        (result.getStatusCode, responseBody)
      } finally {
        response.close()
      }      
    } finally {
      httpClient.close()
    }
  }
}
