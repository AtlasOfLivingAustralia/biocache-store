package au.org.ala.biocache.model

import au.org.ala.biocache.poso.POSO

import scala.beans.BeanProperty

/**
 * POSO representing a conservation status.
 */
class ConservationStatus (
  @BeanProperty var region:String,
  @BeanProperty var regionId:String,
  @BeanProperty var status:String,
  @BeanProperty var rawStatus:String
  ) extends POSO {
  def this() = this(null, null, null, null)
  override def toString = region + " - status:" + status + " - raw:" + rawStatus
}
