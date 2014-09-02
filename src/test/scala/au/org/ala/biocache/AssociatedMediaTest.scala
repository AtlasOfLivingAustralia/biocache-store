package au.org.ala.biocache

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import au.org.ala.biocache.load.{Loader, DataLoader}

@RunWith(classOf[JUnitRunner])
class AssociatedMediaTest extends ConfigFunSuite {

  test("Comma separated list with full URLs"){
    val associatedMedia = "http://static.inaturalist.org/photos/20812/medium.jpg, http://static.inaturalist.org/photos/20813/medium.jpg"
    val loader = new Loader()
    val urls = loader.unpackAssociatedMedia(associatedMedia)
    expectResult(2){ urls.size }
    expectResult("http://static.inaturalist.org/photos/20812/medium.jpg"){ urls(0) }
    expectResult("http://static.inaturalist.org/photos/20813/medium.jpg"){ urls(1) }
  }

  test("Semi-colon separated list with full URLs"){
    val associatedMedia = "http://static.inaturalist.org/photos/20812/medium.jpg; http://static.inaturalist.org/photos/20813/medium.jpg"
    val loader = new Loader()
    val urls = loader.unpackAssociatedMedia(associatedMedia)
    expectResult(2){ urls.size }
    expectResult("http://static.inaturalist.org/photos/20812/medium.jpg"){ urls(0) }
    expectResult("http://static.inaturalist.org/photos/20813/medium.jpg"){ urls(1) }
  }

  test("Comma separated list with relative paths"){
    val associatedMedia = "medium1.jpg, medium2.jpg"
    val loader = new Loader()
    val urls = loader.unpackAssociatedMedia(associatedMedia)
    expectResult(2){ urls.size }
    expectResult("medium1.jpg"){ urls(0) }
    expectResult("medium2.jpg"){ urls(1) }

  }

  test("Semi-colon separated list with relative paths"){
    val associatedMedia = "medium1.jpg; medium2.jpg"
    val loader = new Loader()
    val urls = loader.unpackAssociatedMedia(associatedMedia)
    expectResult(2){ urls.size }
    expectResult("medium1.jpg"){ urls(0) }
    expectResult("medium2.jpg"){ urls(1) }
  }

  test("URLs with comma in single URL"){
    val associatedMedia = "http://static.inaturalist.org/photos/20812/med,ium.jpg"
    val loader = new Loader()
    val urls = loader.unpackAssociatedMedia(associatedMedia)
    expectResult(1){ urls.size }
    expectResult("http://static.inaturalist.org/photos/20812/med,ium.jpg"){ urls(0) }
  }
}