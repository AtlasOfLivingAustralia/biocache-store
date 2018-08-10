package au.org.ala.biocache.vocab

/**
 * A vocabulary matcher for datums.
 */
object GeodeticDatum extends Vocab {

  val all = loadVocabFromFile("/datums.txt")

  /**
    * Match a term. Matches canonical form or variants in array
    *
    * @param string2Match
    * @return
    */
  override def matchTerm(string2Match: String) : Option[Term] = {
    if(string2Match == null)
      return None

    //clean up
    val cleanedStr = string2Match.replaceAll("[(|)\\.]", "")
    var result = super.matchTerm(cleanedStr)

    //seems to be common for datums to be passed in WGS84/GDA94 format
    if(result.isEmpty){
      result = findBySplitting(cleanedStr, "/")
      if (result.isEmpty)
        result = findBySplitting(cleanedStr, " ")
      if (result.isEmpty)
        result = findBySplitting(cleanedStr, "-")

      result
    } else {
      result
    }
  }

  def findBySplitting(str:String, delimiter:String): Option[Term] ={
    val parts = str.split(delimiter)
    if (parts.length > 1){
      var found:Option[Term] = None
      var i = 0
      while(found.isEmpty && i < parts.length){
        found = super.matchTerm(parts(i))
        i += 1
      }
      found
    } else {
      None
    }
  }

}
