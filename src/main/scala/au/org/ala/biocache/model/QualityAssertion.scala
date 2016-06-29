package au.org.ala.biocache.model

import au.org.ala.biocache.poso.POSO
import java.util.{Date, UUID}
import scala.beans.BeanProperty
import au.org.ala.biocache.vocab.{AssertionCodes, ErrorCode, AssertionStatus}
import au.org.ala.biocache.util.BiocacheConversions

/**
 * A companion object for the QualityAssertion class that provides factory
 * type functionality.
 */
object QualityAssertion {
  import BiocacheConversions._
  def apply(code:Int) = {
    val uuid = UUID.randomUUID.toString
    val errorCode = AssertionCodes.getByCode(code)
    if(errorCode.isEmpty){
      throw new Exception("Unrecognised code: " + code)
    }
    new QualityAssertion(uuid,null,errorCode.get.name,errorCode.get.code,null,null,2,null,null,null,null,null,null,null,null,new Date())
  }

  def apply(errorCode:ErrorCode) = {
    val uuid = UUID.randomUUID.toString
    new QualityAssertion(uuid,null,errorCode.name,errorCode.code,null,null,0,null,null,null,null,null,null,null,null,new Date())
  }
  def apply(errorCode:ErrorCode,problemAsserted:Boolean) = {
    val uuid = UUID.randomUUID.toString
    new QualityAssertion(uuid,null,errorCode.name,errorCode.code,null,null,if(problemAsserted) 0 else 1,null,null,null,null,null,null,null,null,new Date())
  }
  def apply(errorCode:ErrorCode,problemAsserted:Boolean,comment:String) = {
    val uuid = UUID.randomUUID.toString
    new QualityAssertion(uuid,null,errorCode.name,errorCode.code,null,null,if(problemAsserted) 0 else 1,comment,null,null,null,null,null,null,null,new Date())
  }
  def apply(errorCode:ErrorCode,comment:String) = {
    val uuid = UUID.randomUUID.toString
    new QualityAssertion(uuid,null,errorCode.name,errorCode.code,null,null,0,comment,null,null,null,null,null,null,null,new Date())
  }
  def apply(errorCode:ErrorCode, qaStatus:Int, comment:String)={
    val uuid = UUID.randomUUID.toString
    new QualityAssertion(uuid,null, errorCode.name, errorCode.code,null,null, qaStatus, comment, null,null,null,null,null,null,null,new Date())
  }
  def apply(errorCode:ErrorCode, qaStatus:Int)={
    val uuid = UUID.randomUUID.toString
    new QualityAssertion(uuid,null, errorCode.name, errorCode.code,null,null, qaStatus, null, null,null,null,null,null,null,null,new Date())
  }
  def apply(assertionCode:Int,problemAsserted:Boolean,comment:String) = {
    val uuid = UUID.randomUUID.toString
    new QualityAssertion(uuid,null,null,assertionCode,null,null,if(problemAsserted) 0 else 1,comment,null,null,null,null,null,null,null,new Date())
  }
  def apply(assertionCode:Int, qaStatus:Int, comment:String) ={
    val uuid = UUID.randomUUID().toString
    new QualityAssertion(uuid,null, null, assertionCode,null,null,qaStatus,comment,null,null,null,null,null,null,null,new Date())
  }
  def apply(assertionCode:Int, qaStatus:Int) ={
    val uuid = UUID.randomUUID().toString
    new QualityAssertion(uuid,null, null, assertionCode,null,null,qaStatus,null,null,null,null,null,null,null,null,new Date())
  }

  def compareByRelatedId = (a:QualityAssertion, b:QualityAssertion) => {
    a.relatedUuid < b.relatedUuid
  }

  def compareByCreatedDesc = (a:QualityAssertion, b:QualityAssertion) => {
    val dtf =org.joda.time.format.DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
    dtf.parseDateTime(a.created).isAfter(dtf.parseDateTime(b.created))
  }

  def compareByReferenceRowKeyDesc = (a:QualityAssertion, b:QualityAssertion) => {
    a.referenceRowKey > b.referenceRowKey
  }

}

/**
 * Quality Assertions are made by man or machine.
 * Man - provided through a UI, giving a positive or negative assertion
 * Machine - provided through backend processing
 */
class QualityAssertion (
  @BeanProperty var uuid:String,
  @BeanProperty var referenceRowKey:String,
  @BeanProperty var name:String,
  @BeanProperty var code:Int,
  @Deprecated var problemAsserted:java.lang.Boolean,
  @BeanProperty var relatedUuid:String, // Uuid of the related assertion if this is verified assertion
  @BeanProperty var qaStatus:Int,//either 0-failed, 1-passed, 2-not tested for System Assertions  and (50001 - 50005) for User Assertions records
  @BeanProperty var comment:String,
  @BeanProperty var value:String,
  @BeanProperty var userId:String, //null for system assertions
  @BeanProperty var userEmail:String,  //null for system assertions
  @BeanProperty var userDisplayName:String,  //null for system assertions
  @BeanProperty var userRole:String,  //null for system assertions, example - collection manager
  @BeanProperty var userEntityUid:String,  //null for system assertions, example - co13
  @BeanProperty var userEntityName:String,  //null for system assertions, example - ANIC
  @BeanProperty var created:String)
  extends Cloneable with Comparable[AnyRef] with POSO {

 // override def toString :String = s"name:$name, code:$code, value:$value, comment:$comment, qaStatus:$qaStatus, relatedUuid:$relatedUuid"
  override def toString :String = s"code:$code, qaStatus:$getQAStatusName, uuid:$uuid, relatedUuid:$relatedUuid, created:$created \n"

  def this() = this(null,null,null,-1,false,null,2,null,null,null,null,null,null,null,null,null)
  override def clone : QualityAssertion = super.clone.asInstanceOf[QualityAssertion]
  override def equals(that: Any) = that match {
    case other: QualityAssertion => {
      (other.code == code) && (other.problemAsserted == problemAsserted) && (other.userId == userId) && (other.qaStatus == qaStatus)
    }
    case _ => false
  }

  def getQAStatusName = {
    AssertionStatus.getQAAssertionName(qaStatus.toString)
  }

  /**
   * NC a temporary measure so that the qaStatus is correctly set for historic records.
   * @param asserted
   */
  def setProblemAsserted(asserted:java.lang.Boolean){
    problemAsserted = asserted
    qaStatus = if(asserted) 0 else 1
  }

  def getProblemAsserted = problemAsserted

  def compareTo(qa:AnyRef) = -1
}