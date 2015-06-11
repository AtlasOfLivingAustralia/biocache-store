package au.org.ala.biocache

import java.util

import org.scalatest.FunSuite

/**
 * Created by mar759 on 4/06/15.
 */
class StoreRecordIT extends FunSuite {

  test("Store a record"){

    import scala.collection.JavaConversions._

    val properties = new java.util.HashMap[String, String]()
    properties.put("scientificName", "Macropus rufus")
    properties.put("occurrenceID", "my-occurrence-id")

    val multimedia = new java.util.ArrayList[java.util.Map[String, String]]
    multimedia.add(Map[String, String](
      "identifier" -> "http://bie.ala.org.au/repo/1013/128/1280064/smallRaw.jpg",
      "creator" -> "David Martin",
      "title" -> "My Test Image Title 1",
      "rights" -> "My Test Image Title",
      "rightsHolder" -> "My Test Image Title",
      "license" -> "Creative Commons Attribution",
      "description" -> "This is a description of Test Image 1"
    ))
    multimedia.add(Map[String, String](
      "identifier" -> "http://bie.ala.org.au/repo/1036/40/404471/raw.jpg",
      "creator" -> "David Martin",
      "title" -> "My Test Image Title 2",
      "rights" -> "My test rights",
      "rightsHolder" -> "I Dave Martin, am the rights holder",
      "license" -> "Creative Commons Zero",
      "description" -> "This is a description of Test Image 2"
    ))
    Store.upsertRecord("dr0", properties, multimedia, true)
  }
}
