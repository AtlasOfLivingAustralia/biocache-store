package au.org.ala.biocache

import au.org.ala.biocache.util.LayersStore
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.client.{RemoteMappingBuilder, ScenarioMappingBuilder}
import com.github.tomakehurst.wiremock.junit.WireMockStaticRule
import org.junit.Rule
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner



@RunWith(classOf[JUnitRunner])
class LayersStoreTest extends ConfigFunSuite {
  val _wireMockRule = new WireMockStaticRule(8112); // Needed for junit 4.8.2
  @Rule
  def wireMockRule = _wireMockRule; // Deranged interaction between scala and junit rules

  def testLayersService = "http://spatial.ala.org.au/layers-service"
  def mockLayersService = "http://localhost:8112"

  test("get fieldIds") {
    val layersStore: LayersStore = new LayersStore(testLayersService)
    val fields: java.util.List[String] = layersStore.getFieldIds()

    expectResult(true) {
      fields.size() > 0
    }
  }

  test("samplingStatus Finished") {
    val layersStore = new LayersStore(mockLayersService)
    val path = "/intersect/batch/1467769914087"
    val body = "{\"progress\": 2, \"progressMessage\": \"Finished sampling layer: aus1. Points processed: 1\", \"status\": \"finished\", \"downloadUrl\": \"http://spatial.ala.org.au/layers-service/intersect/batch/download/1467769914087\", \"finished\": \"06/07/16 11:51:56:532\", \"points\": 2, \"started\": \"06/07/16 11:51:56:439\", \"fields\": 1 }"
    val response = aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(body)
    val getMap = get(urlEqualTo(path)).willReturn(response).asInstanceOf[RemoteMappingBuilder[_ <: AnyRef, _ <: ScenarioMappingBuilder]]
    stubFor(getMap)

    val (statusCode, responseBody, retry) = layersStore.samplingStatus(mockLayersService + path)
    expectResult(200) { statusCode }
    expectResult(body) { responseBody }
    expectResult(false) { retry }
  }

  test("samplingStatus Error") {
    val layersStore = new LayersStore(mockLayersService)
    val path = "/intersect/batch/1467769914087"
    val body = "{\"progress\": 2, \"progressMessage\": \"Finished sampling layer: aus1. Points processed: 1\", \"status\": \"error\", \"downloadUrl\": \"http://spatial.ala.org.au/layers-service/intersect/batch/download/1467769914087\", \"finished\": \"06/07/16 11:51:56:532\", \"points\": 2, \"started\": \"06/07/16 11:51:56:439\", \"fields\": 1 }"
    val response = aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(body)
    val getMap = get(urlEqualTo(path)).willReturn(response).asInstanceOf[RemoteMappingBuilder[_ <: AnyRef, _ <: ScenarioMappingBuilder]]
    stubFor(getMap)

    val (statusCode, responseBody, retry) = layersStore.samplingStatus(mockLayersService + path)
    expectResult(200) { statusCode }
    expectResult(body) { responseBody }
    expectResult(false) { retry }
  }

  test("samplingStatus Waiting") {
    val layersStore = new LayersStore(mockLayersService)
    val path = "/intersect/batch/1467769914087"
    val body = "{\"progress\": 2, \"progressMessage\": \"Finished sampling layer: aus1. Points processed: 1\", \"status\": \"waiting\", \"downloadUrl\": \"http://spatial.ala.org.au/layers-service/intersect/batch/download/1467769914087\", \"finished\": \"06/07/16 11:51:56:532\", \"points\": 2, \"started\": \"06/07/16 11:51:56:439\", \"fields\": 1 }"
    val response = aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(body)
    val getMap = get(urlEqualTo(path)).willReturn(response).asInstanceOf[RemoteMappingBuilder[_ <: AnyRef, _ <: ScenarioMappingBuilder]]
    stubFor(getMap)

    val (statusCode, responseBody, retry) = layersStore.samplingStatus(mockLayersService + path)
    expectResult(200) { statusCode }
    expectResult(body) { responseBody }
    expectResult(true) { retry }
  }

  test("samplingStatus No-bind") {
    val layersStore = new LayersStore(mockLayersService)
    val path = "/intersect/batch/1467769914087"
    val body = "{\"progress\": 2, \"progressMessage\": \"Finished sampling layer: aus1. Points processed: 1\", \"status\": \"waiting\", \"downloadUrl\": \"http://spatial.ala.org.au/layers-service/intersect/batch/download/1467769914087\", \"finished\": \"06/07/16 11:51:56:532\", \"points\": 2, \"started\": \"06/07/16 11:51:56:439\", \"fields\": 1 }"
    val response = aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(body)
    val getMap = get(urlEqualTo(path)).willReturn(response).asInstanceOf[RemoteMappingBuilder[_ <: AnyRef, _ <: ScenarioMappingBuilder]]
    stubFor(getMap)

    val (statusCode, responseBody, retry) = layersStore.samplingStatus("http://nowhere.in.particular")
    expectResult(500) { statusCode }
    expectResult(null) { responseBody }
    expectResult(false) { retry }
  }

  test("samplingStatus No-connect") {
    val layersStore = new LayersStore(mockLayersService)
    val path = "/intersect/batch/1467769914087"
    val body = "{\"progress\": 2, \"progressMessage\": \"Finished sampling layer: aus1. Points processed: 1\", \"status\": \"waiting\", \"downloadUrl\": \"http://spatial.ala.org.au/layers-service/intersect/batch/download/1467769914087\", \"finished\": \"06/07/16 11:51:56:532\", \"points\": 2, \"started\": \"06/07/16 11:51:56:439\", \"fields\": 1 }"
    val response = aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(body)
    val getMap = get(urlEqualTo(path)).willReturn(response).asInstanceOf[RemoteMappingBuilder[_ <: AnyRef, _ <: ScenarioMappingBuilder]]
    stubFor(getMap)

    val (statusCode, responseBody, retry) = layersStore.samplingStatus("http://localhost:8113" + path)
    expectResult(503) { statusCode }
    expectResult(null) { responseBody }
    expectResult(true) { retry }
  }

  test("sample OK") {
    val layersStore = new LayersStore(mockLayersService)
    val batchPath = "/intersect/batch"
    val statusPath = "/intersect/batch/1467769914087"
    val downloadPath = "/intersect/batch/download/1467769914087"
    val batchBody = "{ \"statusUrl\": \"" + mockLayersService + statusPath + "\"  }"
    val batchResponse = aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(batchBody)
    val batchMap = post(urlEqualTo(batchPath)).willReturn(batchResponse).asInstanceOf[RemoteMappingBuilder[_ <: AnyRef, _ <: ScenarioMappingBuilder]]
    val statusBody = "{\"progress\": 2, \"progressMessage\": \"Finished sampling layer: aus1. Points processed: 1\", \"status\": \"finished\", \"downloadUrl\": \"" + mockLayersService + downloadPath + "\", \"finished\": \"06/07/16 11:51:56:532\", \"points\": 2, \"started\": \"06/07/16 11:51:56:439\", \"fields\": 1 }"
    val statusResponse = aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(statusBody)
    val statusMap = get(urlEqualTo(statusPath)).willReturn(statusResponse).asInstanceOf[RemoteMappingBuilder[_ <: AnyRef, _ <: ScenarioMappingBuilder]]
    val downloadBody = "I should be a zip file"
    val downloadResponse = aResponse().withStatus(200).withHeader("Content-Type", "application/zip").withBody(downloadBody)
    val downloaDMap = get(urlEqualTo(downloadPath + "?csv=true")).willReturn(downloadResponse).asInstanceOf[RemoteMappingBuilder[_ <: AnyRef, _ <: ScenarioMappingBuilder]]
    stubFor(batchMap)
    stubFor(statusMap)
    stubFor(downloaDMap)

    val reader = layersStore.sample(Array("cl22"), Array(Array(29.911,132.769),Array(-20.911,122.769)), null)
    expectResult(false) { reader == null }
    expectResult('I') { reader.read }
    expectResult(' ') { reader.read }
    expectResult('s') { reader.read }
  }

  test("sample waiting") {
    val layersStore = new LayersStore(mockLayersService)
    val batchPath = "/intersect/batch"
    val statusPath = "/intersect/batch/1467769914087"
    val downloadPath = "/intersect/batch/download/1467769914087"
    val batchBody = "{ \"statusUrl\": \"" + mockLayersService + statusPath + "\"  }"
    val batchResponse = aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(batchBody)
    val batchMap = post(urlEqualTo(batchPath)).willReturn(batchResponse).asInstanceOf[RemoteMappingBuilder[_ <: AnyRef, _ <: ScenarioMappingBuilder]]
    val statusBody = "{\"progress\": 2, \"progressMessage\": \"Finished sampling layer: aus1. Points processed: 1\", \"status\": \"waiting\", \"downloadUrl\": \"" + mockLayersService + downloadPath + "\", \"finished\": \"06/07/16 11:51:56:532\", \"points\": 2, \"started\": \"06/07/16 11:51:56:439\", \"fields\": 1 }"
    val statusResponse = aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(statusBody)
    val statusMap = get(urlEqualTo(statusPath)).willReturn(statusResponse).asInstanceOf[RemoteMappingBuilder[_ <: AnyRef, _ <: ScenarioMappingBuilder]]
    val downloadBody = "I should be a zip file"
    val downloadResponse = aResponse().withStatus(200).withHeader("Content-Type", "application/zip").withBody(downloadBody)
    val downloaDMap = get(urlEqualTo(downloadPath + "?csv=true")).willReturn(downloadResponse).asInstanceOf[RemoteMappingBuilder[_ <: AnyRef, _ <: ScenarioMappingBuilder]]
    stubFor(batchMap)
    stubFor(statusMap)
    stubFor(downloaDMap)

    val reader = layersStore.sample(Array("cl22"), Array(Array(29.911,132.769),Array(-20.911,122.769)), null)
    expectResult(true) { reader == null }
  }

  test("sample no-connect") {
    val layersStore = new LayersStore(mockLayersService)
    val batchPath = "/intersect/batch"
    val statusPath = "/intersect/batch/1467769914087"
    val downloadPath = "/intersect/batch/download/1467769914087"
    val batchBody = "{ \"statusUrl\": \"" + "http://localhost:8113" + statusPath + "\"  }"
    val batchResponse = aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(batchBody)
    val batchMap = post(urlEqualTo(batchPath)).willReturn(batchResponse).asInstanceOf[RemoteMappingBuilder[_ <: AnyRef, _ <: ScenarioMappingBuilder]]
    val statusBody = "{\"progress\": 2, \"progressMessage\": \"Finished sampling layer: aus1. Points processed: 1\", \"status\": \"waiting\", \"downloadUrl\": \"" + mockLayersService + downloadPath + "\", \"finished\": \"06/07/16 11:51:56:532\", \"points\": 2, \"started\": \"06/07/16 11:51:56:439\", \"fields\": 1 }"
    val statusResponse = aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(statusBody)
    val statusMap = get(urlEqualTo(statusPath)).willReturn(statusResponse).asInstanceOf[RemoteMappingBuilder[_ <: AnyRef, _ <: ScenarioMappingBuilder]]
    val downloadBody = "I should be a zip file"
    val downloadResponse = aResponse().withStatus(200).withHeader("Content-Type", "application/zip").withBody(downloadBody)
    val downloaDMap = get(urlEqualTo(downloadPath + "?csv=true")).willReturn(downloadResponse).asInstanceOf[RemoteMappingBuilder[_ <: AnyRef, _ <: ScenarioMappingBuilder]]
    stubFor(batchMap)
    stubFor(statusMap)
    stubFor(downloaDMap)

    val reader = layersStore.sample(Array("cl22"), Array(Array(29.911,132.769),Array(-20.911,122.769)), null)
    expectResult(true) { reader == null }
  }

  //Only use this test on a fast sampling layers-service
//  test("sample across all fields") {
//    val layersStore: LayersStore = new LayersStore(testLayersService)
//    val strings: java.util.List[String] = layersStore.getFieldIds()
//    val doubles: Array[Array[Double]] =  Array(Array(131.1, -22.2), Array(140, -23.7))
//
//    val sample: java.util.List[Array[String]] = new CSVReader(layersStore.sample(strings.toArray(Array.ofDim(strings.size())),doubles)).readAll()
//
//    expectResult(true) {
//      sample.size() == 3 && sample.get(0).length > 2
//    }
//  }
}