package au.org.ala.biocache

import java.util

import au.com.bytecode.opencsv.CSVReader
import au.org.ala.biocache.caches.LocationDAO
import au.org.ala.biocache.util.LayersStore
import org.junit.runner.RunWith
import org.scalatest.Ignore
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LayersStoreTest extends ConfigFunSuite {

  def testLayersService = "http://spatial.ala.org.au/layers-service"

  test("get fieldIds") {
    val layersStore: LayersStore = new LayersStore(testLayersService)
    val fields: java.util.List[String] = layersStore.getFieldIds()

    expectResult(true) {
      fields.size() > 0
    }
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