package au.org.ala.biocache.processor

import au.org.ala.biocache.model.{FullRecord, QualityAssertion}
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
/**
  * Created by koh032 on 1/06/2016.
  */
class IdentificationQualifierProcessor extends Processor {

  val logger = LoggerFactory.getLogger("Indentification Qualifier Processor")

  def process(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {

    val (translatedIdQualifier, assertions) = processIdentificationQualifier (raw.identification.identificationQualifier)
    processed.identification.identificationQualifier = translatedIdQualifier

    val (translatedAbcdIdQualifier, abcdAssertions) = processIdentificationQualifier (raw.identification.abcdIdentificationQualifier)
    processed.identification.abcdIdentificationQualifier = translatedAbcdIdQualifier

    (assertions ++ abcdAssertions).toArray
  }

  def processIdentificationQualifier(rawIdentificationQualifier:String) : (String, ArrayBuffer[QualityAssertion]) = {

    val assertions = new ArrayBuffer[QualityAssertion]

    val CERTAIN_KEYWORD_LIST = "\\bcertain\\b|\\bconfident\\b|\\bconfirm(?:ed)?\\b|\\bpositive\\b|\\bverified\\b"
    val CertainKeywordRegex = ("((?:.*?)?(?:" + CERTAIN_KEYWORD_LIST + ")(?:.*)?)").r

    val UNCERTAIN_KEYWORD_LIST = "\\bcf\\b[.]|\\baff\\b[.]|\\?|\\bunknown\\b|\\bnot certain\\b|\\bnegative\\b|\\buncertain\\b|" +
      "\\buncertainty\\b|\\bincorrect\\b|\\bpossible\\b|\\bprobable\\b|\\bsp\\b[.]|\\bsp\\b|\\bunnamed\\b|" +
      "\\bunsure\\b|\\bforsan\\b|\\bnear\\b|\\bx\\b|\\bnot sure\\b|\\bnot confirm(?:ed)?\\b|\\bnot correct\\b".r
    val UncertainKeywordRegex = ("((?:.*?)?(?:" + UNCERTAIN_KEYWORD_LIST + ")(?:.*)?)").r

    val stringVal = rawIdentificationQualifier

    // default value
    var translatedIdQ = "no value provided"

    logger.debug ("Processing raw.identification.identificationQualifier: " + stringVal)

    if (StringUtils.isNotBlank(stringVal)) {
      stringVal toLowerCase match {
        case UncertainKeywordRegex (keyword)  => logger.debug ("Identification Qualifier '" + stringVal + "' is mapped to Uncertain"); translatedIdQ = "Uncertain"
        case CertainKeywordRegex (keyword) => logger.debug ("Identification Qualifier '" + stringVal + "' is mapped to Certain"); translatedIdQ = "Certain"
        case _ => logger.debug ("Identification Qualifier '" + stringVal + "' is mapped to Not recognised"); translatedIdQ = "Not recognised"
      }
    }

    return (translatedIdQ, assertions)
  }

  def getName = "identification"
}
