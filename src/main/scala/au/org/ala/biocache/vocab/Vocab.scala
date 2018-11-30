package au.org.ala.biocache.vocab

import java.io.File
import au.org.ala.biocache.Config
import au.org.ala.biocache.util.Stemmer
import org.slf4j.LoggerFactory

/**
 * A trait for a vocabulary. A vocabulary consists of a set
 * of Terms, each with string variants.
 */
trait Vocab {

  import scala.collection.JavaConversions._

  val all:Set[Term]

  val logger = LoggerFactory.getLogger("Vocab")

  val regexNorm = """[^a-zA-Z0-9]+"""

  def getStringList : java.util.List[String] = all.map(t => t.canonical).toList.sorted

  /**
   * Match a term. Matches canonical form or variants in array
   * @param string2Match
   * @return
   */
  def matchTerm(string2Match:String) : Option[Term] = {
    if(string2Match != null){
      //strip whitespace & strip quotes and fullstops & uppercase
      val stringToUse = string2Match.replaceAll(regexNorm, "").toLowerCase
      val stemmed = Stemmer.stem(stringToUse)

      //println("string to use: " + stringToUse)
      all.foreach(term => {
        //println("matching to term " + term.canonical)
        if(term.canonical.equalsIgnoreCase(stringToUse))
          return Some(term)
        if(term.variants.contains(stringToUse) || term.variants.contains(stemmed)){
          return Some(term)
        }
      })
    }
    None
  }

  def retrieveCanonicals(terms:Seq[String]) = {
    terms.map(ch => {
        DwC.matchTerm(ch) match {
            case Some(term) => term.canonical
            case None => ch
        }
    })
  }

  def retrieveCanonicalsOrNothing(terms:Seq[String]) = {
    terms.map(ch => {
        DwC.matchTerm(ch) match {
            case Some(term) => term.canonical
            case None => ""
        }
    })
  }

  def loadVocabFromVerticalFile(filePath:String) : Set[Term] = {

    val map = getSource(filePath).getLines.toList.map { row =>
        val values = row.split("\t")
        val variant = values.head.replaceAll(regexNorm, "").toLowerCase
        val canonical = values.last
        (variant, canonical)
    }.toMap

    val grouped = map.groupBy({ case(k,v) => v })

    grouped.map { case(canonical, valueMap) =>
       val variants = valueMap.keys
       new Term(canonical, variants.toArray)
    }.toSet
  }

  def loadVocabFromFile(filePath:String) : Set[Term] = getSource(filePath).getLines.toList.map({ row =>
    val values = row.split("\t")
    val variants = values.map(x => x.replaceAll(regexNorm, "").toLowerCase).filter( x => x != "")
    new Term(values.head, variants)
  }).toSet

  private def getSource(filePath:String) : scala.io.Source = {
    val overrideFile = new File(Config.vocabDirectory + filePath)
    if(overrideFile.exists){
      //if external file exists, use this
      logger.info("Reading vocab file: " + overrideFile.getAbsolutePath)
      scala.io.Source.fromFile(overrideFile, "utf-8")
    } else {
      //else use the file shipped with jar
      logger.info("Reading internal vocab file: " + filePath)
      scala.io.Source.fromURL(getClass.getResource(filePath), "utf-8")
    }
  }

  /**
   * Retrieve all the terms defined in this vocab.
   * @return
   */
  def retrieveAll : Set[Term] = {
    val methods = this.getClass.getMethods
    (for {
      method <- methods
      if (method.getReturnType == classOf[Term])
    } yield (method.invoke(this).asInstanceOf[Term])).toSet[Term]
  }
}