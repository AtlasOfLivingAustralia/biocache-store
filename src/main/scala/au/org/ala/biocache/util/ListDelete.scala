package au.org.ala.biocache.util

import au.org.ala.biocache.tool.RecordDeletor

/**
 * RecordDeletor that takes a list of rowkeys.
 */
class ListDelete(rowKeys:List[String]) extends RecordDeletor {

  override def deleteFromPersistent() = {
    rowKeys.foreach(rowKey=>{
      //pm.delete(rowKey, "occ")
      //use the occ DAO to delete so that the record is added to the dellog cf
      occurrenceDAO.delete(rowKey,false,true)
    })
  }
  override def deleteFromIndex {
   val query = "row_key:\"" + rowKeys.mkString("\" OR row_key:\"") +"\""
   indexer.removeByQuery(query)
  }
}
