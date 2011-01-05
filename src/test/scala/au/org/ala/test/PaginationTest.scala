package au.org.ala.test

import java.io._
import org.apache.cassandra.thrift._
import org.apache.cassandra.thrift.Column
import org.apache.cassandra.thrift.SuperColumn
import scala.collection.mutable.LinkedList
import scala.Application
import org.apache.cassandra.thrift.ConsistencyLevel
import org.apache.cassandra.thrift.ColumnPath
import org.apache.thrift.transport.TTransport
import scala.reflect._
import java.io._
import java.util.ArrayList
import scala.collection.immutable.Set
import scala.collection.mutable.ListBuffer
import se.scalablesolutions.akka.persistence.cassandra._
import se.scalablesolutions.akka.persistence.common._
import org.apache.cassandra.thrift.ConsistencyLevel
import org.apache.cassandra.thrift.ColumnPath
import org.apache.thrift.transport.TTransport
import java.util.UUID

import org.wyki.cassandra.pelops.Mutator
import org.wyki.cassandra.pelops.Pelops
import org.wyki.cassandra.pelops.Policy
import org.wyki.cassandra.pelops.Selector

object PaginationTest {
	
  def main(args : Array[String]) : Unit = {
     val hosts = Array{"localhost"}
     Pelops.addPool("test-pool",hosts, 9160, false, "occurrence", new Policy());
     val selector = Pelops.createSelector("test-pool", "occurrence");
     val slicePredicate = new SlicePredicate
    
     
     val columnNames = new ArrayList[Array[Byte]]
     columnNames.add("key".getBytes)
     slicePredicate.setColumn_names(columnNames)
		
		// page through all dr1 style keys
      var startKey = ""
	  var keyRange = Selector.newKeyRange("dr3@", "", 101) 	
		
	  val columnMap = selector.getColumnsFromRows(keyRange, "dr", slicePredicate, ConsistencyLevel.ONE)
	  
		  if(columnMap.size>0) {
		 	  //get the array of keys
  			  val columnsObj = List(columnMap.keySet.toArray : _*)
			  val columns = columnsObj.asInstanceOf[List[String]]
  			  //print last 
		 	  for(key<-columns){
		 	 	  
		 	 	  if(key.startsWith("dr3@")){
		 	 		  println(key)
		 	 	  }
		 	  }  
		 		  
			  keyRange = Selector.newKeyRange(startKey, "", 101)
		  } 
	  
	  Pelops.shutdown
	}
}