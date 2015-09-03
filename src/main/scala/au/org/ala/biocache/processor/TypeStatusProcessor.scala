package au.org.ala.biocache.processor

import au.org.ala.biocache.model.{FullRecord, QualityAssertion}
import au.org.ala.biocache.vocab.{AssertionCodes, AssertionStatus, TypeStatus}
import org.slf4j.LoggerFactory

/**
 * Process type status information
 */
class TypeStatusProcessor extends Processor {

  import AssertionCodes._
  import AssertionStatus._
  val logger = LoggerFactory.getLogger("TypeStatusProcessor")


  val WORD_PATTERN = ( """[\p{L}]{4,}""").r


   /**
   * Process the type status
   */
  def process(guid: String, raw: FullRecord, processed: FullRecord,lastProcessed: Option[FullRecord]=None): Array[QualityAssertion] = {

     if (raw.identification.typeStatus != null && !raw.identification.typeStatus.isEmpty) {

       // Examining the all words having more than 4 characters to match with a predefined type status
       val typeStatuses = WORD_PATTERN.findAllIn(raw.identification.typeStatus).toList.map( word => { TypeStatus.matchTerm(word) })
       val list =  typeStatuses.filter(_.nonEmpty).map(ts => ts.get.canonical).distinct

       if (list.isEmpty) {
         //add a quality assertion
         Array(QualityAssertion(UNRECOGNISED_TYPESTATUS, "Unrecognised type status"))
       } else {
         processed.identification.typeStatus = list mkString "|"
         Array(QualityAssertion(UNRECOGNISED_TYPESTATUS, PASSED))
       }
     } else {
       Array()
     }
   }



  def getName = "type"
}
