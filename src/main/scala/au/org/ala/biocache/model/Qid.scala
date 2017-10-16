package au.org.ala.biocache.model

import au.org.ala.biocache.util.Json
import org.codehaus.jackson.annotate.JsonIgnore

import scala.beans.BeanProperty

class Qid(@BeanProperty var rowKey: String, @BeanProperty var q: String, @BeanProperty var displayString: String,
          @BeanProperty var wkt: String, @BeanProperty var bbox: Array[Double],
          @BeanProperty var lastUse: Long, @BeanProperty var fqs: Array[String],
          @BeanProperty var maxAge: Long, @BeanProperty var source: String) {

  def this(map: Map[String, String] = Map[String, String]()) = {
    this(map.getOrElse("rowkey", null),
      map.getOrElse("q", null),
      map.getOrElse("displayString", null),
      map.getOrElse("wkt", null),
      if (map.getOrElse("bbox", null) != null) Json.toStringArray(map("bbox").toString).map(d => d.toDouble)
      else null,
      if (map.getOrElse("lastUse", null) != null) map("lastUse").toString.toLong
      else System.currentTimeMillis(),
      if (map.getOrElse("fqs", null) != null) Json.toStringArray(map("fqs").toString)
      else null,
      if (map.getOrElse("maxAge", null) != null) map("maxAge").toString.toLong
      else -1,
      map.getOrElse("source", null))
  }

  @JsonIgnore
  lazy val size = {
    var size = 0
    if (q != null) {
      size += q.getBytes.length
    }
    if (displayString != null) {
      size += displayString.getBytes.length
    }
    if (wkt != null) {
      size += wkt.getBytes.length
    }
    if (bbox != null) {
      size += 4 * 4
    }
    if (fqs != null) {
      for (fq <- fqs) {
        size += fq.getBytes.length
      }
    }
    if (source != null) {
      size += source.getBytes.length
    }
    size + 8 + 8 + 8
  }

  def toMap: Map[String, String] = Map("rowkey" -> rowKey, "q" -> q, "displayString" -> displayString,
    "wkt" -> wkt, "bbox" -> Json.toJSON(bbox), "lastUse" -> lastUse.toString, "fqs" -> Json.toJSON(fqs),
    "maxAge" -> maxAge.toString, "source" -> source)
}