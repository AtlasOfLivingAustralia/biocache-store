package au.org.ala.biocache.vocab

/**
 * Mime types for matching multimedia formats.
 *
 * @author Doug Palmer &lt;Doug.Palmer@csiro.au&gt;

 *         Copyright (c) 2015 CSIRO
 */
object MimeType extends Vocab {
  val all = loadVocabFromFile("/mime-types.txt")
}
