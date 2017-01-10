package au.org.ala.biocache

import au.org.ala.biocache.model.QualityAssertion
import au.org.ala.biocache.processor.LocationProcessor
import au.org.ala.biocache.util.{GridRef, GridUtil}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class GridReferenceTest extends FunSuite {

  test("Convert OS grid reference to Northing / Easting") {

    expectResult(Some(GridRef("NM", 130000, 790000, Some(10000), 130000, 790000, 140000, 800000, "EPSG:27700"))) {
      GridUtil.gridReferenceToEastingNorthing("NM39")
    }

    expectResult(Some(GridRef("NM",140000,799000, Some(1000), 140000, 799000, 141000, 800000, "EPSG:27700"))) {
      GridUtil.gridReferenceToEastingNorthing("NM4099")
    }

    expectResult(Some(GridRef("NG",131600,800500, Some(100),131600,800500,131700,800600, "EPSG:27700"))) {
      GridUtil.gridReferenceToEastingNorthing("NG316005")
    }

    expectResult(Some(GridRef("NM",130000,790000,Some(2000),130000,790000,132000,792000, "EPSG:27700"))) {
      GridUtil.gridReferenceToEastingNorthing("NM39A")
    }

    expectResult(Some(GridRef("NM",130000,798000,Some(2000),130000,798000,132000,800000, "EPSG:27700"))) {
      GridUtil.gridReferenceToEastingNorthing("NM39E")
    }

    expectResult(Some(GridRef("NM",132000,792000,Some(2000),132000,792000,134000,794000, "EPSG:27700"))) {
      GridUtil.gridReferenceToEastingNorthing("NM39G")
    }

    expectResult(Some(GridRef("NM",136000,794000,Some(2000),136000,794000,138000,796000, "EPSG:27700"))) {
      GridUtil.gridReferenceToEastingNorthing("NM39S")
    }

    expectResult(Some(GridRef("NM",134000,796000,Some(2000),134000,796000,136000,798000, "EPSG:27700"))) {
      GridUtil.gridReferenceToEastingNorthing("NM39N")
    }

    expectResult(Some(GridRef("NM",134000,798000,Some(2000),134000,798000,136000,800000, "EPSG:27700"))) {
      GridUtil.gridReferenceToEastingNorthing("NM39P")
    }

    expectResult(Some(GridRef("NM",138000,798000,Some(2000),138000,798000,140000,800000, "EPSG:27700"))) {
      GridUtil.gridReferenceToEastingNorthing("NM39Z")
    }
  }

  test("Convert irish grid reference to Northing / Easting") {
    val result1 = GridUtil.processGridReference("J4967")
    expectResult("54.52944") { result1.get.minLatitude.toString }  //bottom left of the grid
    expectResult("-5.69914") { result1.get.minLongitude.toString }  //bottom left of the grid

    val result2 = GridUtil.processGridReference("IJ4967")
    expectResult("54.52944") { result2.get.minLatitude.toString }  //bottom left of the grid
    expectResult("-5.69914") { result2.get.minLongitude.toString }  //bottom left of the grid

    val result3 = GridUtil.processGridReference("H99")
    expectResult("390000") { result3.get.northing.toString }  //bottom left of the grid
    expectResult("290000") { result3.get.easting.toString }  //bottom left of the grid
    expectResult("-6.5238") { result3.get.longitude.toString }  //bottom left of the grid
    expectResult("54.79388") { result3.get.latitude.toString }  //bottom left of the grid
  }

  test("Convert OS grid reference to decimal latitude/longitude in WGS84") {
    val result = GridUtil.processGridReference("NM39")
    expectResult(false) { result.isEmpty }
    expectResult("56.97001") { result.get.latitude.toString }
    expectResult("-6.36199") { result.get.longitude.toString }
    expectResult("EPSG:4326") { result.get.datum.toString }
    expectResult("10000") { result.get.coordinateUncertaintyInMeters.toString }
  }

  test("NH1234123 at different resolutions") {
    val map = GridUtil.getGridRefAsResolutions("NH123123")
    expectResult("NH") { map.get("grid_ref_100000") }
    expectResult("NH11") { map.get("grid_ref_10000") }
    expectResult("NH1212") { map.get("grid_ref_1000") }
    expectResult("NH123123") { map.get("grid_ref_100") }
  }

  test("NH12341234 at different resolutions") {
    val map = GridUtil.getGridRefAsResolutions("NH12341234")
    expectResult("NH") { map.get("grid_ref_100000") }
    expectResult("NH11") { map.get("grid_ref_10000") }
    expectResult("NH1212") { map.get("grid_ref_1000") }
    expectResult("NH123123") { map.get("grid_ref_100") }
  }

  test("NH1234512345 at different resolutions") {
    val map = GridUtil.getGridRefAsResolutions("NH1234512345")
    expectResult("NH") { map.get("grid_ref_100000") }
    expectResult("NH11") { map.get("grid_ref_10000") }
    expectResult("NH11G") { map.get("grid_ref_2000") }
    expectResult("NH1212") { map.get("grid_ref_1000") }
    expectResult("NH123123") { map.get("grid_ref_100") }
  }

  test("J12341234 at different resolutions") {
    val map = GridUtil.getGridRefAsResolutions("J12341234")
    expectResult("J") { map.get("grid_ref_100000") }
    expectResult("J11") { map.get("grid_ref_10000") }
    expectResult("J1212") { map.get("grid_ref_1000") }
    expectResult("J123123") { map.get("grid_ref_100") }
  }

  test("J43214321 at different resolutions") {
    val map = GridUtil.getGridRefAsResolutions("J43214321")
    expectResult("J") { map.get("grid_ref_100000") }
    expectResult("J44") { map.get("grid_ref_10000") }
    expectResult("J44G") { map.get("grid_ref_2000") }
    expectResult("J4343") { map.get("grid_ref_1000") }
    expectResult("J432432") { map.get("grid_ref_100") }
  }

  test("Dogfood at different resolutions - J43G") {
     GridUtil.gridReferenceToEastingNorthing("J43G") match {
       case Some(gr) => {
         val gridref = gr.gridLetters + gr.easting.toString().substring(1) + gr.northing.toString().substring(1)
         val map = GridUtil.getGridRefAsResolutions(gridref)
         expectResult("J") { map.get("grid_ref_100000") }
         expectResult("J43") { map.get("grid_ref_10000") }
         expectResult("J43G") { map.get("grid_ref_2000") }
      }
    }
  }

  test("Dogfood at different resolutions - C12Q") {
    GridUtil.gridReferenceToEastingNorthing("C12Q") match {
      case Some(gr) => {
        val gridref = gr.gridLetters + gr.easting.toString().substring(1) +gr. northing.toString().substring(1)
        val map = GridUtil.getGridRefAsResolutions(gridref)
        expectResult("C") { map.get("grid_ref_100000") }
        expectResult("C12") { map.get("grid_ref_10000") }
        expectResult("C12Q") { map.get("grid_ref_2000") }
      }
    }
  }

  test("Dogfood at different resolutions - NH12Q") {
    GridUtil.gridReferenceToEastingNorthing("NH12Q") match {
      case Some(gr) => {
        val gridref = gr.gridLetters + gr.easting.toString().substring(1) + gr.northing.toString().substring(1)
        val map = GridUtil.getGridRefAsResolutions(gridref)
        expectResult("NH") { map.get("grid_ref_100000") }
        expectResult("NH12") { map.get("grid_ref_10000") }
        expectResult("NH12Q") { map.get("grid_ref_2000") }
      }
    }
  }

  test("J1212 at different resolutions") {
    val map = GridUtil.getGridRefAsResolutions("J1212")
    expectResult("J") { map.get("grid_ref_100000") }
    expectResult("J11") { map.get("grid_ref_10000") }
    expectResult("J1212") { map.get("grid_ref_1000") }
  }

  test("J11 at different resolutions") {
    val map = GridUtil.getGridRefAsResolutions("J11")
    expectResult("J") { map.get("grid_ref_100000") }
    expectResult("J11") { map.get("grid_ref_10000") }
  }

  test("J at different resolutions") {
    val map = GridUtil.getGridRefAsResolutions("J")
    expectResult("J") { map.get("grid_ref_100000") }
  }

  test("NH at different resolutions") {
    val map = GridUtil.getGridRefAsResolutions("NH")
    expectResult("NH") { map.get("grid_ref_100000") }
    expectResult(false) { map.get("grid_ref_10000")  == "NH00"}
    expectResult(true) { map.get("grid_ref_10000")  == null}
  }

  test("NF8359 at different resolutions") {
    val map = GridUtil.getGridRefAsResolutions("NF8359")
    expectResult("NF") { map.get("grid_ref_100000") }
    expectResult("NF85") { map.get("grid_ref_10000") }
    expectResult("NF8359") { map.get("grid_ref_1000") }
  }

  test("HU35  at different resolutions") {
    val map = GridUtil.getGridRefAsResolutions("HU35")
    expectResult("HU") { map.get("grid_ref_100000") }
    expectResult("HU35") { map.get("grid_ref_10000") }
  }

  test("HY 489 020  at different resolutions") {
    val map = GridUtil.getGridRefAsResolutions("HY489020")
    expectResult("HY") { map.get("grid_ref_100000") }
    expectResult("HY40") { map.get("grid_ref_10000") }
//    expectResult("HY4802") { map.get("grid_ref_2000") }
    expectResult("HY4802") { map.get("grid_ref_1000") }
    expectResult("HY489020") { map.get("grid_ref_100") }
  }


}
