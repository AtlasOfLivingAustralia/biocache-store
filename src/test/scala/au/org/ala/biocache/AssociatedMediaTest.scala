package au.org.ala.biocache

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import au.org.ala.biocache.load.{DownloadMedia, Loader, DataLoader}

@RunWith(classOf[JUnitRunner])
class AssociatedMediaTest extends ConfigFunSuite {

  test("Another list test"){
    val associatedMedia = "http://static.inaturalist.org/photos/140018/medium.JPG, http://static.inaturalist.org/photos/140019/medium.JPG"
    val urls = DownloadMedia.unpackAssociatedMedia(associatedMedia)
    expectResult(2){ urls.size }
    expectResult("http://static.inaturalist.org/photos/140018/medium.JPG"){ urls(0) }
    expectResult("http://static.inaturalist.org/photos/140019/medium.JPG"){ urls(1) }
  }

  test("Comma separated list with full URLs"){
    val associatedMedia = "http://static.inaturalist.org/photos/20812/medium.jpg, http://static.inaturalist.org/photos/20813/medium.jpg"
    val urls = DownloadMedia.unpackAssociatedMedia(associatedMedia)
    expectResult(2){ urls.size }
    expectResult("http://static.inaturalist.org/photos/20812/medium.jpg"){ urls(0) }
    expectResult("http://static.inaturalist.org/photos/20813/medium.jpg"){ urls(1) }
  }

  test("Semi-colon separated list with full URLs"){
    val associatedMedia = "http://static.inaturalist.org/photos/20812/medium.jpg; http://static.inaturalist.org/photos/20813/medium.jpg"
    val urls = DownloadMedia.unpackAssociatedMedia(associatedMedia)
    expectResult(2){ urls.size }
    expectResult("http://static.inaturalist.org/photos/20812/medium.jpg"){ urls(0) }
    expectResult("http://static.inaturalist.org/photos/20813/medium.jpg"){ urls(1) }
  }

  test("Bar list with multiple protocols"){
    val associatedMedia = "http://static.inaturalist.org/photos/20812/medium.jpg | https://static.inaturalist.org/photos/20813/medium.jpg | ftp://static.inaturalist.org/photos/20814/medium.jpg | ftps://static.inaturalist.org/photos/20815/medium.jpg |"
    val urls = DownloadMedia.unpackAssociatedMedia(associatedMedia)
    expectResult(4){ urls.size }
    expectResult("http://static.inaturalist.org/photos/20812/medium.jpg"){ urls(0) }
    expectResult("https://static.inaturalist.org/photos/20813/medium.jpg"){ urls(1) }
    expectResult("ftp://static.inaturalist.org/photos/20814/medium.jpg"){ urls(2) }
    expectResult("ftps://static.inaturalist.org/photos/20815/medium.jpg"){ urls(3) }
  }

  test("Comma separated list with relative paths"){
    val associatedMedia = "medium1.jpg, medium2.jpg"
    val urls = DownloadMedia.unpackAssociatedMedia(associatedMedia)
    expectResult(2){ urls.size }
    expectResult("medium1.jpg"){ urls(0) }
    expectResult("medium2.jpg"){ urls(1) }

  }

  test("Semi-colon separated list with relative paths"){
    val associatedMedia = "medium1.jpg; medium2.jpg"
    val urls = DownloadMedia.unpackAssociatedMedia(associatedMedia)
    expectResult(2){ urls.size }
    expectResult("medium1.jpg"){ urls(0) }
    expectResult("medium2.jpg"){ urls(1) }
  }

  test("URLs with comma in single URL"){
    val associatedMedia = "http://static.inaturalist.org/photos/20812/med,ium.jpg"
    val urls = DownloadMedia.unpackAssociatedMedia(associatedMedia)
    expectResult(1){ urls.size }
    expectResult("http://static.inaturalist.org/photos/20812/med,ium.jpg"){ urls(0) }
  }
}