package au.org.ala.biocache.model

import scala.beans.BeanProperty

class DuplicateRecordDetails(@BeanProperty var rowKey:String,
                             @BeanProperty var uuid:String,
                             @BeanProperty var taxonConceptLsid:String,
                             @BeanProperty var year:String,
                             @BeanProperty var month:String,
                             @BeanProperty var day:String,
                             @BeanProperty var point1:String,
                             @BeanProperty var point0_1:String,
                             @BeanProperty var point0_01:String,
                             @BeanProperty var point0_001:String,
                             @BeanProperty var point0_0001:String,
                             @BeanProperty var latLong:String,
                             @BeanProperty var rawScientificName:String,
                             @BeanProperty var collector:String,
                             @BeanProperty var oldStatus:String,
                             @BeanProperty var oldDuplicateOf:String,
                             @BeanProperty var recordNumber:String,
                             @BeanProperty var catalogueNumber:String){

  def this() = this(null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)

  @BeanProperty var status = "U"
  @BeanProperty var druid:String = {
    if(rowKey != null){
      rowKey.split("\\|")(0)
    } else {
      null
    }
  }
  @BeanProperty var duplicateOf:String = null
  @BeanProperty var precision = 0
  @BeanProperty var duplicates:Array[DuplicateRecordDetails] = Array[DuplicateRecordDetails]()
  @BeanProperty var dupTypes:Array[DupType] = Array[DupType]()
}