package au.org.ala.biocache.tool

import au.org.ala.biocache.Config
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.model.QualityAssertion
import au.org.ala.biocache.util.Json
import org.apache.http.MethodNotSupportedException

/**
  * Copies the QA's from the occ list to the QA column family.
  */
object RetrofitQAColumnFamily {

  def main(args: Array[String]) = {
    //TODO: update for cassandra3/solr6

    throw new RuntimeException("not implemented");

    val pm = Config.persistenceManager
    val theClass = classOf[QualityAssertion].asInstanceOf[java.lang.Class[AnyRef]]
    pm.pageOverSelect("occ", (rowKey, map) => {
      //get the list
      if (map contains "userQualityAssertion") {
        val listJson = map.getOrElse("userQualityAssertion", "[]")
        val qalist: List[QualityAssertion] = Json.toListWithGeneric(listJson, theClass)
        //add each assertion on the list to the QA column family
        qalist.foreach(qa => {
          val qaRowKey = rowKey + "|" + qa.getUserId + "|" + qa.getCode
          pm.put(qaRowKey, "qa", FullRecordMapper.mapObjectToProperties(qa), true, false)
        })
      }
      true
    }, "", "", 1000, "uuid", "rowkey", "userQualityAssertion")
  }
}