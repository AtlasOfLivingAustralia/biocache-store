package au.org.ala.biocache.dao

import au.org.ala.biocache.model.{DuplicateRecordDetails, Qid}
import au.org.ala.biocache.persistence.PersistenceManager
import au.org.ala.biocache.util.Json
import com.google.inject.Inject
import org.scale7.cassandra.pelops.UuidHelper
import org.slf4j.LoggerFactory

/**
 * Created by mar759 on 17/02/2014.
 */
class QidDAOImpl extends QidDAO {

  protected val logger = LoggerFactory.getLogger("QidDAO")
  @Inject
  var persistenceManager: PersistenceManager = _

  var lastId: Long = System.currentTimeMillis()

  def get(qid:String): Qid = {
    def map = persistenceManager.get(qid, "qid")

    if(map.isDefined) {
      var m = map.get
      if ("null".equals(m.getOrElse("bbox", "null").toString)) {
        m = m - "bbox"
      }
      def qid = new Qid(m)

      //update lastUse time and commit if >5min difference
      if (qid.lastUse + 5 * 60 * 1000 < System.currentTimeMillis()) {
        put(qid)
      }

      qid
    } else {
      null
    }
  }

  def put(qid: Qid) : Qid = {
    qid.lastUse = System.currentTimeMillis()

    if (qid.rowKey == null || qid.rowKey.isEmpty) {
      qid.rowKey = nextId.toString
    }

    persistenceManager.put(qid.rowKey, "qid", qid.toMap.filter(_._2 != null))

    qid
  }

  /**
   * qid's had numeric ids (long), want to keep the same so nothing breaks
   */
  def nextId() : Long = {
    this.synchronized {
      var id: Long = System.currentTimeMillis()
      if (id == lastId) id = id + 1
      lastId = id
      id
    }
  }
}
