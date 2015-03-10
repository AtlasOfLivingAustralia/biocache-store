package au.org.ala.biocache

import au.org.ala.biocache.caches.TaxonProfileDAO
import au.org.ala.biocache.dao.{QidDAO, OccurrenceDAO}
import au.org.ala.biocache.model.{Qid, ConservationStatus, TaxonProfile}
import au.org.ala.biocache.parser.VerbatimLatLongParser
import au.org.ala.biocache.util.Json
import au.org.ala.biocache.vocab.BasisOfRecord
import com.google.inject.Guice
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.slf4j.LoggerFactory

/**
 * All tests should extend this to access the correct DI
 */
@RunWith(classOf[JUnitRunner])
class QidTest extends FunSuite {

  val logger = LoggerFactory.getLogger("ConfigFunSuite")
  System.setProperty("biocache.config","/biocache-test-config.properties")

  Config.inj = Guice.createInjector(new TestConfigModule)
  val pm = Config.persistenceManager
  println("Loading up test suite with persistencemanager - " + pm.getClass.getName)

  val qidDAO = Config.getInstance(classOf[QidDAO]).asInstanceOf[QidDAO]

  test("Qid put and get") {
    val qid = new Qid()
    qid.bbox = Array(112, -44, 154, -12)
    qid.q = "*:*"
    qid.fqs = Array("longitude:*", "latitude:*")
    qid.maxAge = 50000

    def newQid = qidDAO.put(qid)

    println("Created qid with uuid=" + newQid.rowKey)

    def getQid = qidDAO.get(newQid.rowKey)

    expectResult(true) {
      getQid.rowKey == newQid.rowKey &&
        Json.toJSON(getQid.bbox).equals(Json.toJSON(qid.bbox)) &&
        Json.toJSON(getQid.fqs).equals(Json.toJSON(qid.fqs)) &&
        getQid.maxAge == qid.maxAge &&
        getQid.q.equals(qid.q)
    }
  }
}