/**************************************************************************
 *  Copyright (C) 2010 Atlas of Living Australia
 *  All Rights Reserved.
 * 
 *  The contents of this file are subject to the Mozilla Public
 *  License Version 1.1 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://www.mozilla.org/MPL/
 * 
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 ***************************************************************************/
package au.org.ala.util
import java.io.File
import java.util.UUID
import org.apache.cassandra.thrift._
import org.gbif.dwc.terms._
import org.gbif.dwc.text._
import org.wyki.cassandra.pelops.{ Pelops, Policy }
import scala.reflect._

/**
 * Reads a DwC-A and writes the data to the BioCache
 * 
 * @author Dave Martin (David.Martin@csiro.au)
 */
object DwCLoader {

  val hosts = Array { "localhost" }
  val keyspace = "occurrence"
  val columnFamily = "occurrence"
  val poolName = "test-pool"
  val resourceUid = "dp20"

  def main(args: Array[String]): Unit = {

    println("Starting DwC loader....")
    val file = new File("/Users/davejmartin2/Desktop/ozcam/")
    val archive = ArchiveFactory.openArchive(file)
    val iter = archive.iteratorDwc
    val terms = DwcTerm.values

    Pelops.addPool(poolName, hosts, 9160, false, keyspace, new Policy);
    val drPath = new ColumnPath("dr")
    val occurrencePath = new ColumnPath(columnFamily)

    var count = 0

    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis

    while (iter.hasNext) {
      count += 1
      //the newly assigned record UUID
      val dwc = iter.next

      //the details of how to construct the UniqueID belong in the Collectory
      val cc: String = dwc.getProperty(DwcTerm.collectionCode)
      val ic: String = dwc.getProperty(DwcTerm.institutionCode)
      val cn: String = dwc.getProperty(DwcTerm.catalogNumber)
      val uniqueID = resourceUid + "|" + cc + "|" + ic + "|" + cn

      //check if we already have this
      var uuidPath = new ColumnPath("dr")
      uuidPath.setColumn("uuid".getBytes)

      //lookup the column
      val recordUuid = createOrRetrieveUuid(uniqueID)

      //write all the supplied properties
      val mutator = Pelops.createMutator(poolName, columnFamily)
      for (term <- terms) {
        val property = dwc.getProperty(term)
        if (property != null && property.trim.length > 0) {
          //store the property
          val colName = (term.simpleName).getBytes
          mutator.writeColumn(recordUuid, columnFamily, mutator.newColumn(term.simpleName, property))
        }
      }

      //commit the writes
      mutator.execute(ConsistencyLevel.ONE)

      //debug
      if (count % 1000 == 0 && count > 0) {
        finishTime = System.currentTimeMillis
        println(count + ", >> last key : " + uniqueID + ", UUID: " + recordUuid + ", records per sec: " + 1000f / (((finishTime - startTime).toFloat) / 1000f))
        startTime = System.currentTimeMillis
      }
    }
    Pelops.shutdown
    println("Finished DwC loader.")
  }

  /**
   * 
   */
  def createOrRetrieveUuid(uniqueID: String): String = {
    try {
      val selector = Pelops.createSelector(poolName, columnFamily)
      val column = selector.getColumnFromRow(uniqueID, "dr", "uuid".getBytes, ConsistencyLevel.ONE)
      if (column != null) {
        new String(column.value)
      } else {
        // write new UUID to table
        val newUuid = UUID.randomUUID.toString
        val mutator = Pelops.createMutator(poolName, columnFamily)
        mutator.writeColumn(uniqueID, columnFamily, mutator.newColumn("uuid".getBytes, newUuid))
        newUuid
      }
    } catch {
      case e =>
        None
        UUID.randomUUID.toString
    }
  }
}
