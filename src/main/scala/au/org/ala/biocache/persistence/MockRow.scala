package au.org.ala.biocache.persistence

class MockRow(data:collection.Map[String, String]) extends DataRow {

  val keys = data.keySet.toList
  val values = keys.map { key => data.getOrElse(key, null)}

  override def getIndexOf(str: String) = keys.indexOf(str)
  override def getString(idx: Int) = values(idx)
  override def getName(idx: Int) = keys(idx)
  override def getNumberOfFields() = keys.size
}
