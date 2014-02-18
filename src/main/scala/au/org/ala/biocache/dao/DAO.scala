package au.org.ala.biocache.dao

import java.util.UUID

/**
 * Created by mar759 on 17/02/2014.
 */
trait DAO {
  def createUuid = UUID.randomUUID.toString
}
