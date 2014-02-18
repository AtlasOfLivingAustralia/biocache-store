package au.org.ala.biocache.model

import scala.beans.BeanProperty

/**
 * Created by mar759 on 17/02/2014.
 */
class ConservationSpecies(
  @BeanProperty var region:String,
  @BeanProperty var regionId:String,
  @BeanProperty var status:String,
  @BeanProperty var rawStatus:String
  ){
  def this() = this(null, null, null,null)
}
