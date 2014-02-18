package au.org.ala.biocache.dao

/**
 * Created by mar759 on 17/02/2014.
 */
trait DeletedRecordDAO {

  /**
   * @param startDate must be in the form yyyy-MM-dd
   */
  def getUuidsForDeletedRecords(startDate:String) : Array[String]
}
