package au.org.ala.biocache

import java.net.URL

import au.org.ala.biocache.model.Multimedia
import org.gbif.dwc.terms.{DcTerm, Term}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MultimediaTest extends ConfigFunSuite {
  test("Test find mime type format 1"){
    val metadata: Map[Term, String] = Map(DcTerm.format -> "image/jpeg")
    expectResult("image/jpeg") {
      Multimedia.findMimeType(metadata)
    }
  }

  test("Test find mime type format 2"){
    val metadata: Map[Term, String] = Map(DcTerm.format -> "jpeg")
    expectResult("image/jpeg") {
      Multimedia.findMimeType(metadata)
    }
  }

  test("Test find mime type format 3"){
    val metadata: Map[Term, String] = Map(DcTerm.format -> "mp3")
    expectResult("audio/mp3") {
      Multimedia.findMimeType(metadata)
    }
  }

  test("Test find mime type format 4"){
    val metadata: Map[Term, String] = Map(DcTerm.format -> "something-odd")
    expectResult("something-odd") {
      Multimedia.findMimeType(metadata)
    }
  }

  test("Test find mime type identifier 1"){
    val metadata: Map[Term, String] = Map(DcTerm.identifier -> "fred.jpg")
    expectResult("image/jpeg") {
      Multimedia.findMimeType(metadata)
    }
  }

  test("Test find mime type identifier 2"){
    val metadata: Map[Term, String] = Map(DcTerm.identifier -> "fred.PNG")
    expectResult("image/png") {
      Multimedia.findMimeType(metadata)
    }
  }

  test("Test find mime type identifier 3"){
    val metadata: Map[Term, String] = Map(DcTerm.identifier -> "fred.PNG?a-parameter")
    expectResult("image/png") {
      Multimedia.findMimeType(metadata)
    }
  }

  test("Test find mime type identifier 4"){
    val metadata: Map[Term, String] = Map(DcTerm.identifier -> "fred.blah")
    expectResult("image/*") {
      Multimedia.findMimeType(metadata)
    }
  }

  test("Test find mime type empty 1"){
    val metadata: Map[Term, String] = Map()
    expectResult("image/*") {
      Multimedia.findMimeType(metadata)
    }
  }

  test("Test create 1"){
    val metadata: Map[Term, String] = Map(DcTerm.format -> "jpeg", DcTerm.identifier -> "fred.jpg", DcTerm.license -> "Public")
    val location = new URL("http://localhost/fred.jpg")
    val multimedia = Multimedia.create(location, metadata)
    expectResult(location){ multimedia.location }
    expectResult("image/jpeg"){ multimedia.mediaType }
    expectResult(3){ multimedia.metadata.size }
    expectResult("fred.jpg"){ multimedia.metadata(DcTerm.identifier) }
  }

  test("Test move 1"){
    val metadata: Map[Term, String] = Map(DcTerm.format -> "jpeg", DcTerm.identifier -> "fred.jpg", DcTerm.license -> "Public")
    val location = new URL("http://localhost/fred.jpg")
    val location2 = new URL("http://localhost/fred2.jpg")
    val multimedia = Multimedia.create(location, metadata)
    val multimedia2 = multimedia.move(location2)
    expectResult(location2){ multimedia2.location }
    expectResult("image/jpeg"){ multimedia2.mediaType }
    expectResult(3){ multimedia2.metadata.size }
    expectResult("fred.jpg"){ multimedia2.metadata(DcTerm.identifier) }
  }

  test("Test addMetadata 1"){
    val metadata: Map[Term, String] = Map(DcTerm.format -> "jpeg", DcTerm.identifier -> "fred.jpg", DcTerm.license -> "Public")
    val location = new URL("http://localhost/fred.jpg")
    val multimedia = Multimedia.create(location, metadata)
    val multimedia2 = multimedia.addMetadata(DcTerm.alternative, "Nothing")
    expectResult(location){ multimedia2.location }
    expectResult("image/jpeg"){ multimedia2.mediaType }
    expectResult(4){ multimedia2.metadata.size }
    expectResult("Nothing"){ multimedia2.metadata(DcTerm.alternative) }
  }

  test("Test addMetadata 2"){
    val metadata: Map[Term, String] = Map(DcTerm.format -> "jpeg", DcTerm.identifier -> "fred.jpg", DcTerm.license -> "Public")
    val location = new URL("http://localhost/fred.jpg")
    val multimedia = Multimedia.create(location, metadata)
    val multimedia2 = multimedia.addMetadata(DcTerm.identifier, "fred2.jpg")
    expectResult(location){ multimedia2.location }
    expectResult("image/jpeg"){ multimedia2.mediaType }
    expectResult(3){ multimedia2.metadata.size }
    expectResult("fred2.jpg" ){ multimedia2.metadata(DcTerm.identifier) }
  }

  test("Test metadataAsStrings 1"){
    val metadata: Map[Term, String] = Map(DcTerm.format -> "jpeg", DcTerm.identifier -> "fred.jpg", DcTerm.license -> "Public")
    val location = new URL("http://localhost/fred.jpg")
    val multimedia = Multimedia.create(location, metadata)
    val result = multimedia.metadataAsStrings
    expectResult("jpeg"){ result(DcTerm.format.simpleName) }
    expectResult("Public"){ result(DcTerm.license.simpleName) }
    expectResult(None){ result.get("nothing") }
  }

}
