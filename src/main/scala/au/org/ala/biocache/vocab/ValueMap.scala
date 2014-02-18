package au.org.ala.biocache.vocab

/**
 * Created by mar759 on 18/02/2014.
 */
trait ValueMap {

  var map:Map[String,String] = _

  def loadFromFile(filePath:String): Map[String, String] = {
    scala.io.Source.fromURL(getClass.getResource(filePath), "utf-8").getLines.toList.map({ row =>
      val values = row.split("\t")
      values(0) -> values(1)
    }).toMap
  }
}
