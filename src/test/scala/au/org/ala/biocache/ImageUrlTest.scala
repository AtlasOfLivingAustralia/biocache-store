package au.org.ala.biocache

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfterAll
import au.org.ala.biocache.load.MediaStore

@RunWith(classOf[JUnitRunner])
class ImageUrlTest extends ConfigFunSuite with BeforeAndAfterAll {

  test("/dr340/2224/5b76a871-c3fc-4394-9559-3f6fd627512b/O-74049_Eopsaltria_australis-1.jpg"){
    expectResult(true) {
      Config.mediaStore.isValidImageURL("file:///data/biocache-media/dr340/2224/5b76a871-c3fc-4394-9559-3f6fd627512b/NNSWII02-37_Thalassoma_lunare.jpg")
    }
  }
}
