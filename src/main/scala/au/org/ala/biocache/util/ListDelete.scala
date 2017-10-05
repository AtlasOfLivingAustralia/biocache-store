package au.org.ala.biocache.util

import au.org.ala.biocache.tool.RecordDeletor

/**
  * RecordDeletor that takes a list of rowkeys.
  */
class ListDelete(rowKeys: List[String]) extends RecordDeletor {

  override def deleteFromPersistent() = {
    rowKeys.foreach(rowKey => {
      //pm.delete(rowKey, "occ")
      //use the occ DAO to delete so that the record is added to the dellog cf
      occurrenceDAO.delete(rowKey, false, true)
    })
  }

  override def deleteFromIndex {
    //page over rowKeys to limit OR terms in the SOLR query
    var start = 0
    val pageSize = 1000
    while (start < rowKeys.length) {
      val subList = rowKeys.slice(start, Math.min(start + pageSize, rowKeys.length))
      val query = "row_key:\"" + subList.mkString("\" OR row_key:\"") + "\""
      indexer.removeByQuery(query)

      start += pageSize
    }

  }
}
