package au.org.ala.biocache

import java.util.UUID

import au.org.ala.biocache.model.QualityAssertion
import au.org.ala.biocache.persistence.Cassandra3PersistenceManager
import au.org.ala.biocache.vocab.AssertionCodes._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Cassandra3PersistenceTests extends FunSuite {


    test("Get by RowKey"){
      val pm = new Cassandra3PersistenceManager("127.0.0.1", 9042, "occ")
      //uuid:String, entityName:String, idxColumn:String
      val result = pm.getByIndex("ba393d65-c0f1-449e-b6bc-830f5b31ebe2",  "occ" , "uuid", "rowkey")

      println(result)

    }


//  test("Get by RowKey"){
//    val pm = new Cassandra3PersistenceManager("127.0.0.1", 9042, "occ_test")
//
//    val rowkey = "dr0|" + UUID.randomUUID().toString
//    pm.put(rowkey, "occ_test_fixed", Map("scientificName" -> "Macropus rufus", "dataResourceUID" -> "dr0"), true)
//    pm.get(rowkey, "occ_test_fixed") match {
//      case Some(record) => {
//        println("Got a record")
//        record.foreach { case(key, value) => println(s"$key = $value") }
//      }
//      case None => println("No record")
//    }
//  }
//
//  test("Get by indexed value"){
//    val pm = new Cassandra3PersistenceManager("127.0.0.1", 9042, "occ_test")
//    val rowkey = "dr0|" + UUID.randomUUID().toString
//    val uuid = UUID.randomUUID().toString
//    pm.put(rowkey, "occ_test_fixed", Map("scientificName" -> "Macropus rufus", "dataResourceUID" -> "dr0", "uuid" -> uuid), true)
//    pm.getByIndex(uuid, "occ_test_fixed", "uuid") match {
//      case Some(record) => {
//        println("Got a record")
//        record.foreach { case(key, value) => println(s"$key = $value") }
//      }
//      case None => println("No record")
//    }
//  }
//
//  test("Put list"){
//    val pm = new Cassandra3PersistenceManager("127.0.0.1", 9042, "occ_test")
//
//    val rowkey = "dr0|" + UUID.randomUUID().toString
//    pm.put(rowkey, "occ_test_fixed", Map("scientificName" -> "Macropus rufus", "dataResourceUID" -> "dr0"), true)
//
//    val qalist = List(QualityAssertion(INVALID_SCIENTIFIC_NAME), QualityAssertion(INVALID_GEODETICDATUM))
//    pm.putList[QualityAssertion](rowkey, "occ_test_fixed", "qualityAssertion", qalist, classOf[QualityAssertion], true, false)
//
//    pm.get(rowkey, "occ_test_fixed") match {
//      case Some(record) => {
//        println("Got a record")
//        record.foreach { case(key, value) => println(s"$key = $value") }
//      }
//      case None => println("No record")
//    }
//  }

//  test("Page over all"){
//
//
//  }
//
//  test("Page over by data resource UID"){
//
//
//  }

}
