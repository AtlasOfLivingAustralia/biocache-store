/*
 * Copyright (C) 2012 Atlas of Living Australia
 * All Rights Reserved.
 *
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 */
package au.org.ala.biocache.load

import java.io.{FileOutputStream, File}
import java.net.{URI, URL}
import java.util

import au.org.ala.biocache.util.Json
import au.org.ala.biocache.{Config, ConfigFunSuite}
import au.org.ala.biocache.model.{FullRecord, Multimedia}
import org.apache.commons.io.FileUtils
import org.apache.http.NameValuePair
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.utils.URLEncodedUtils
import org.apache.http.entity.mime.content.{StringBody, FileBody}
import org.apache.http.entity.mime.{HttpMultipartMode, MultipartEntity}
import org.apache.http.impl.client.DefaultHttpClient
import org.gbif.dwc.terms.DcTerm
import org.gbif.dwca.io.ArchiveFactory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable
import scala.io.Source

class TestMediaStore extends MediaStore {
  val media = mutable.Buffer.empty[String]

  def save(uuid: String, resourceUID: String, urlToMedia: String, media: Option[Multimedia]): Option[(String, String)] = {
    media + urlToMedia
    Some((extractFileName(urlToMedia), ""))
  }


  override def extractFileName(urlToMedia: String): String = super.extractFileName(urlToMedia)

  override def getSoundFormats(mediaID: String): java.util.Map[String,String]= {
    val formats = new java.util.HashMap[String, String]()
    formats.put("audio/mpeg", Config.remoteMediaStoreUrl +  "http://localhost/test/" + mediaID )
    formats
  }

  override def convertPathsToUrls(fullRecord: FullRecord, baseUrlPath: String) = if (fullRecord.occurrence.images != null) {
    fullRecord.occurrence.images = fullRecord.occurrence.images.map(x => convertPathToUrl(x, baseUrlPath))
  }

  override def convertPathToUrl(str: String, baseUrlPath: String) = Config.remoteMediaStoreUrl + "/image/proxyImageThumbnail?imageId=" + str

  override def convertPathToUrl(str: String) = Config.remoteMediaStoreUrl + "/image/proxyImageThumbnail?imageId=" + str

  override def getImageFormats(filenameOrID: String): util.Map[String, String] = {
    val formats = new java.util.HashMap[String, String]()
    formats.put("image/jpeg", "http://localhost/test/" + filenameOrID )
    formats
  }

  override def alreadyStored(uuid: String, resourceUID: String, urlToMedia: String): (Boolean, String, String) = (false, "", "")
}


@RunWith(classOf[JUnitRunner])
class MediaStoreTest extends ConfigFunSuite {
  test("is valid image URL 1") {
    val store = new TestMediaStore
    expectResult(true){ store.isValidImageURL("http://localhost/nowhere/nothing1.jpg")}
    expectResult(true){ store.isValidImageURL("https://localhost/nowhere/nothing1.jpg")}
    expectResult(true){ store.isValidImageURL("ftp://localhost/nowhere/nothing1.jpg")}
    expectResult(true){ store.isValidImageURL("ftps://localhost/nowhere/nothing1.jpg")}
    expectResult(true){ store.isValidImageURL("file://localhost/nowhere/nothing1.jpg")}
    expectResult(false){ store.isValidImageURL("mailto://localhost/nowhere/nothing1")}
  }

  test("is valid image URL 2") {
    val store = new TestMediaStore
    expectResult(true){ store.isValidImageURL("http://localhost/nowhere/nothing1.jpg")}
    expectResult(true){ store.isValidImageURL("http://localhost/nowhere/nothing1.gif")}
    expectResult(true){ store.isValidImageURL("http://localhost/nowhere/nothing1.png")}
    expectResult(true){ store.isValidImageURL("http://localhost/nowhere/nothing1.jpeg")}
    expectResult(true){ store.isValidImageURL("http://localhost/nowhere/nothing1.JPG")}
    expectResult(true){ store.isValidImageURL("http://localhost/nowhere/nothing1.GIF")}
    expectResult(true){ store.isValidImageURL("http://localhost/nowhere/nothing1.PNG")}
    expectResult(true){ store.isValidImageURL("http://localhost/nowhere/nothing1.JPEG")}
    expectResult(false){ store.isValidImageURL("http://localhost/nowhere/nothing1")}
    expectResult(false){ store.isValidImageURL("http://localhost/nowhere/nothing1png")}
  }

  test("is valid image URL 3") {
    val store = new TestMediaStore
    expectResult(true){ store.isValidImageURL("http://localhost/nowhere/nothing.jpg?query=five")}
    expectResult(true){ store.isValidImageURL("http://localhost/nowhere/nothing1.gif?something=1&else=2")}
    expectResult(true){ store.isValidImageURL("http://localhost/nowhere/nothing1.png?ext=gif")}
  }

  test("is valid sound URL 1") {
    val store = new TestMediaStore
    expectResult(true){ store.isValidSoundURL("http://localhost/nowhere/nothing1.mp3")}
    expectResult(true){ store.isValidSoundURL("https://localhost/nowhere/nothing1.mp3")}
    expectResult(true){ store.isValidSoundURL("ftp://localhost/nowhere/nothing1.mp3")}
    expectResult(true){ store.isValidSoundURL("ftps://localhost/nowhere/nothing1.mp3")}
  }

  test("is valid sound URL 2") {
    val store = new TestMediaStore
    expectResult(true){ store.isValidSoundURL("http://localhost/nowhere/nothing1.mp3")}
    expectResult(true){ store.isValidSoundURL("http://localhost/nowhere/nothing1.wav")}
    expectResult(true){ store.isValidSoundURL("http://localhost/nowhere/nothing1.ogg")}
    expectResult(true){ store.isValidSoundURL("http://localhost/nowhere/nothing1.flac")}
    expectResult(false){ store.isValidSoundURL("http://localhost/nowhere/nothing1.xxx")}
  }

  test("is valid sound URL 3") {
    val store = new TestMediaStore
    expectResult(true){ store.isValidSoundURL("http://localhost/nowhere/nothing.mp3?query=five")}
    expectResult(true){ store.isValidSoundURL("http://localhost/nowhere/nothing1.wav?something=1&else=2")}
    expectResult(true){ store.isValidSoundURL("http://localhost/nowhere/nothing1.wav?ext=gif")}
  }

  test("is valid video URL 1") {
    val store = new TestMediaStore
    expectResult(true){ store.isValidVideoURL("http://localhost/nowhere/nothing1.wmv")}
    expectResult(true){ store.isValidVideoURL("https://localhost/nowhere/nothing1.wmv")}
    expectResult(true){ store.isValidVideoURL("ftp://localhost/nowhere/nothing1.wmv")}
    expectResult(true){ store.isValidVideoURL("ftps://localhost/nowhere/nothing1.wmv")}
  }

  test("is valid video URL 2") {
    val store = new TestMediaStore
    expectResult(true){ store.isValidVideoURL("http://localhost/nowhere/nothing1.wmv")}
    expectResult(true){ store.isValidVideoURL("http://localhost/nowhere/nothing1.mp4")}
    expectResult(true){ store.isValidVideoURL("http://localhost/nowhere/nothing1.mpg")}
    expectResult(true){ store.isValidVideoURL("http://localhost/nowhere/nothing1.avi")}
    expectResult(true){ store.isValidVideoURL("http://localhost/nowhere/nothing1.mov")}
    expectResult(false){ store.isValidVideoURL("http://localhost/nowhere/nothing1.xxx")}
  }

  test("is valid video URL 3") {
    val store = new TestMediaStore
    expectResult(true){ store.isValidVideoURL("http://localhost/nowhere/nothing.mp4?query=five")}
    expectResult(true){ store.isValidVideoURL("http://localhost/nowhere/nothing1.mp4?something=1&else=2")}
    expectResult(true){ store.isValidVideoURL("http://localhost/nowhere/nothing1.mp4?ext=gif")}
  }

  test("extract file name 1") {
    val store = new TestMediaStore
    expectResult("nothing1.mp3"){ store.extractFileName("http://localhost/nowhere/nothing1.mp3")}
    expectResult("nothing1.mp3"){ store.extractFileName("ftp://localhost/nowhere/nothing1.mp3")}
    expectResult("nothing_1.mp3"){ store.extractFileName("ftp://localhost/nowhere/nothing 1.mp3")}
  }

  test("extract file name 2") {
    val store = new TestMediaStore
    expectResult("nothing1.mp3"){ store.extractFileName("http://localhost/nowhere/nothing1.mp3?query=five")}
    expectResult("nothing1.mp3"){ store.extractFileName("ftp://localhost/nowhere/nothing1.mp3?something=nothing&else=else")}
  }

  test("extract file name 3") {
    val store = new TestMediaStore
    expectResult("elsewhere.jpg"){ store.extractFileName("http://localhost/nowhere/nothing1?fileName=elsewhere.jpg")}
    expectResult("elsewhere.jpg"){ store.extractFileName("ftp://localhost/nowhere/nothing1.jpg?fileName=elsewhere.jpg")}
  }

  test("extract file name 4") {
    val store = new TestMediaStore
    expectResult("raw"){ store.extractFileName("http://localhost/nowhere/nothing1/")}
  }

  test("save media store url id detection") {

    val tests = List(
      ("test valid image/proxyImage", Config.remoteMediaStoreUrl + "/image/proxyImageThumbnailLarge?imageId=119d85b5-76cb-4d1d-af30-e141706be8bf",true),
      ("test invalid image/proxyImage, no imageId",Config.remoteMediaStoreUrl + "/image/proxyImageThumbnailLarge?id=119d85b5-76cb-4d1d-af30-e141706be8bf", false),
      ("test valid store" ,Config.remoteMediaStoreUrl + "/store/e/7/f/3/eb024033-4da4-4124-83f7-317365783f7e/original", true),
      ("test invalid store, bad UUID",Config.remoteMediaStoreUrl + "/store/e/7/f/3/eb024033-4da4-414-83f7-317365783f7e/original", false)
    )

    tests.foreach { it =>
      logger.info(it._1)
      expectResult(it._3) {
        Config.mediaStore.save("", "", it._2, None) match {
          case Some((filename, filepath)) => true
          case None => false
        }
      }
    }
  }
  
}
