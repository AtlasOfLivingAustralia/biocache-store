package au.org.ala.biocache.processor

import java.io.File
import au.org.ala.biocache.Config
import au.org.ala.biocache.model.{FullRecord, QualityAssertion}
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer
/**
  * Created by koh032 on 1/06/2016.
  */
class IdentificationQualifierProcessor extends Processor {

  val logger = LoggerFactory.getLogger(getName)

  val Certain = "Certain"
  val Uncertain = "Uncertain"
  val NotRecognised = "Not recognised"
  val NotProvided = "Not provided"

  val (certainKeywordList, unCertainKeywordList) = readFromFile ("/identificationQualifiers.txt")

  def process(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {

    val (translatedIdQualifier, assertions) = processIdentificationQualifier (raw.identification.identificationQualifier)
    processed.identification.identificationQualifier = translatedIdQualifier

    val (translatedAbcdIdQualifier, abcdAssertions) = processIdentificationQualifier (raw.identification.abcdIdentificationQualifier)
    processed.identification.abcdIdentificationQualifier = translatedAbcdIdQualifier

    (assertions ++ abcdAssertions).toArray
  }

  def processIdentificationQualifier(rawIdentificationQualifier:String) : (String, ArrayBuffer[QualityAssertion]) = {

    val assertions = new ArrayBuffer[QualityAssertion]

    val CertainKeywordRegex = ("((?:.*?)?(?:" + certainKeywordList + ")(?:.*)?)").r

    val UncertainKeywordRegex = ("((?:.*?)?(?:" + unCertainKeywordList + ")(?:.*)?)").r

    val stringVal = rawIdentificationQualifier

    // default value
    var translatedIdQualifier = NotProvided

    logger.debug("Processing raw.identification.identificationQualifier: " + stringVal)

    if (StringUtils.isNotBlank(stringVal)) {
      stringVal.trim.replaceAll("\\s+", " ") toLowerCase match {
        case UncertainKeywordRegex (keyword)  => logger.debug("Identification Qualifier '" + stringVal + "' is mapped to " + Uncertain); translatedIdQualifier = Uncertain
        case CertainKeywordRegex (keyword) => logger.debug("Identification Qualifier '" + stringVal + "' is mapped to " + Certain); translatedIdQualifier = Certain
        case _ => logger.debug("Identification Qualifier '" + stringVal + "' is mapped to " + NotRecognised); translatedIdQualifier = NotRecognised
      }
    } else {
      logger.debug("No Identification Qualifier provided");
    }

    return (translatedIdQualifier, assertions)
  }

  def readFromFile (filePath: String): (String, String) = {

    var certainList = ""
    var uncertainList = ""

    val CertainHeader = "[" + Certain.toLowerCase + "]"
    val UncertainHeader = "[" + Uncertain.toLowerCase + "]"

    var switch = ""

    getSource(filePath).getLines.foreach({ line =>
      logger.debug(line);
      if (StringUtils.isNotBlank(line)) {
        line.trim toLowerCase match {
          case CertainHeader => switch = CertainHeader; logger.debug("Encoutered " + CertainHeader)
          case UncertainHeader => switch = UncertainHeader; logger.debug("Encoutered " + UncertainHeader)
          case _ => if (switch == CertainHeader) {
            if (certainList != "") certainList = certainList.concat("|" + regex(line)) else certainList = certainList.concat(regex(line));
            logger.debug("Adding to certain List ")
          } else if (switch == UncertainHeader) {
            if (uncertainList != "") uncertainList = uncertainList.concat("|" + regex(line)) else uncertainList = uncertainList.concat(regex(line));
            logger.debug("Adding to Uncertain List ")
          }
        }
      }
    })

    logger.debug("Printing certain List from IdentifcationQualifier.txt : " + certainList)
    logger.debug("Printing uncertain List from IdentifcationQualifier.txt : " + uncertainList)

    return (certainList, uncertainList);
  }

  def regex(keyword: String): String = {
    if (keyword == "?") {
      return "\\?"
    } else if (keyword.contains(".")) {
      return "\\b" + keyword.replace(".", "\\b[.]").replace("(ed)", "(?:ed)?").replaceAll("\\s+", " ")
    } else {
      return "\\b" + keyword.replace(".", "[.]").replace("(ed)", "(?:ed)?").replaceAll("\\s+", " ") + "\\b"
    }
  }

  def getSource (filePath: String) : scala.io.Source = {
    val overrideFile = new File(Config.vocabDirectory + filePath)

    if(overrideFile.exists){
      //if external file exists, use this
      logger.debug("Reading identificationQualifier file: " + overrideFile.getAbsolutePath)
      scala.io.Source.fromFile(overrideFile, "utf-8")
    } else {
      //else use the file shipped with jar
      logger.debug("Reading internal identificationQualifier file: " + filePath)
      scala.io.Source.fromURL(getClass.getResource(filePath), "utf-8")
    }

  }

  def getName = "IdentificationQualifierProcessor"
}
