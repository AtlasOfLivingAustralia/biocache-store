package au.org.ala.biocache.load

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer
import au.org.ala.biocache.model.Versions
import au.org.ala.biocache.vocab.DwC

/**
 * A simple loader that loads a map of properties as a record.
 */
class MapDataLoader extends DataLoader {

  import JavaConversions._

  /**
   * Simple loader that takes a map of properties and a set of unique terms.
   *
   * @param dataResourceUid
   * @param values
   * @param uniqueTerms
   * @return
   */
  def load(dataResourceUid:String, values:List[java.util.Map[String,String]], uniqueTerms:List[String]):List[String] = {
    val rowKeys = new ArrayBuffer[String]
    values.foreach(jmap =>{
      val map = jmap.toMap[String,String]
      val uniqueTermsValues = uniqueTerms.map(t => map.getOrElse(t,""))
      //map the keys to DWC values
      val keys = map.keySet.toList
      val biocacheModelValues = DwC.retrieveCanonicals(keys)
      val keysToDwcMap = (keys zip biocacheModelValues).toMap
      val dwcMap = map.map{case (k,v) => (keysToDwcMap.getOrElse(k,k), v)}
      val fr = FullRecordMapper.createFullRecord("", dwcMap, Versions.RAW)
      load(dataResourceUid, fr, uniqueTermsValues, true, true)
      rowKeys += fr.rowKey
    })
    rowKeys.toList
  }
}
