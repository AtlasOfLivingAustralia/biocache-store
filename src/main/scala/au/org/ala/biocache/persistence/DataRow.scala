package au.org.ala.biocache.persistence

trait DataRow {
  def getIndexOf(str:String) : Int
  def getString(idx:Int) : String
  def getName(idx:Int) : String
  def getNumberOfFields() : Int
}
