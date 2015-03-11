package au.org.ala.biocache.dao

import au.org.ala.biocache.model.{Qid, DuplicateRecordDetails}

/**
 * DAO for duplicate records
 */
trait QidDAO {
  def get(qid: String): Qid;

  def put(qid: Qid): Qid;
}
