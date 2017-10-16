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

  def get(url: String): (String) = {
    val result = Source.fromURL(new URL(url)).mkString
    result
  }

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
        logger.debug("Response code: " + result.getStatusCode)
        (result.getStatusCode, responseBody)
      } finally {
        response.close()
      }      
    } finally {
      httpClient.close()
    }
  }

  def uploadFileWithJson(url:String, file:File, contentType:String, json:String) : (Int, String) = {

    //upload an image
    val httpClient = new DefaultHttpClient()
    try {
      val entity = new MultipartEntity(HttpMultipartMode.BROWSER_COMPATIBLE)
      val fileBody = new FileBody(file, contentType)
      entity.addPart("image", fileBody)
      entity.addPart("metadata", new StringBody(json))

      val httpPost = new HttpPost(url)
      httpPost.setEntity(entity)
      val response = httpClient.execute(httpPost)
      try {
        val result = response.getStatusLine()
        logger.debug("Response code: " + result.getStatusCode)
        val responseBody = Source.fromInputStream(response.getEntity().getContent()).mkString
        (result.getStatusCode, responseBody)
      } finally {
        response.close()
      }      
    } finally {
      httpClient.close()
    }
  }
}
