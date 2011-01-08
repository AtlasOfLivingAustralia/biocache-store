package au.org.ala.biocache
import scala.reflect.BeanProperty

/**
 * Quality Assertions are made by man or machine.
 * Man - provided through a UI, giving a positive or negative assertion
 * Machine - provided through backend processing
 * 
 * @author Dave Martin (David.Martin@csiro.au)
 */
class QualityAssertion {
	@BeanProperty var uuid:String = _
	@BeanProperty var assertionCode:Int = _ 
	@BeanProperty var positive:Boolean = _
	@BeanProperty var comment:String = _
	@BeanProperty var userId:String = _
	@BeanProperty var userDisplayName:String = _
	
	override def equals(that: Any) = that match { 
		case other: QualityAssertion => {
			(other.assertionCode == assertionCode) && (other.positive == positive) && (other.userId == userId)
		}
		case _ => false 
 	}
}
//
//case class FieldCorrection (
//		uuid:String, 
//		recordUuid:String, 
//		fieldName:String, 
//		oldValue:String, 
//		newValue:String, 
//		userId:String, 
//		userDisplayName:String)
//
//case class Annotation (
//		uuid:String, 
//		recordUuid:String, 
//		comment:String, 
//		userId:String, 
//		userDisplayName:String, 
//		inReplyToUuid:String)