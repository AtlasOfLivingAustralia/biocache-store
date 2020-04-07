package au.org.ala.biocache.vocab

/**
 * Vocabulary matcher for basis of record values.
 */
object License extends Vocab {
  val all = loadRegexFromFile("/license.txt")
}
