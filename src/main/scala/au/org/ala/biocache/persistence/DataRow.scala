package au.org.ala.biocache.persistence

/**
  * A data row encapsulates a single flat record in its raw form extracted from the
  * underlying storage. The assumption is that implementations of this interface
  * are storing values in an array style data structure.
  */
trait DataRow {
  /**
    * Get the index of the supplied key.
    * @param str
    * @return
    */
  def getIndexOf(str:String) : Int

  /**
    * Get the string value at the specified index.
    * @param idx
    * @return
    */
  def getString(idx:Int) : String

  /**
    * Get the key value at the supplied index.
    * @param idx
    * @return
    */
  def getName(idx:Int) : String

  /**
    * Get the number of fields in this data row.
    * @return
    */
  def getNumberOfFields() : Int
}
