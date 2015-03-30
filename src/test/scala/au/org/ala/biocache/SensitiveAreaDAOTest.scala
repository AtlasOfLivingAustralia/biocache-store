package au.org.ala.biocache

import au.org.ala.biocache.caches.SensitiveAreaDAO
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SensitiveAreaDAOTest extends ConfigFunSuite {

  test("Intersect 149.2, -37.1"){
    val results = SensitiveAreaDAO.intersect(149.2, -37.1)
    println(results)
  }

  test("Intersect 149.2, -22.1"){
    val results = SensitiveAreaDAO.intersect(149.2, -22.1)
    println(results)
  }

  test("Intersect 133.2, -22.1"){
    val results = SensitiveAreaDAO.intersect(133.2, -22.1)
    println(results)
  }
}
