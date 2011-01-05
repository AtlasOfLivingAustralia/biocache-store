package au.org.ala.cluster

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
import org.wyki.cassandra.pelops.Mutator
import org.wyki.cassandra.pelops.Pelops
import org.wyki.cassandra.pelops.Policy
import org.wyki.cassandra.pelops.Selector
import scala.collection.immutable.Set
import scala.collection.mutable.ListBuffer

class LoadPearsonCorrelation {
  
  def superColToList(superColumn:org.apache.cassandra.thrift.SuperColumn) : List[(String,String)] = {
	var cellIds = new ListBuffer[(String,String)]()
	val columns = List(superColumn.getColumns.toArray : _*)
	for(column <- columns){
		val col = column.asInstanceOf[Column]
		cellIds + (( new String(col.getName), new String(col.getValue) )) 
	}
	cellIds.result
  }
  
  def pageOverAll(selector:Selector, slicePredicate:SlicePredicate, proc:((String,List[(String,String)])=>Unit) ): Unit = {
	  var startKey = ""
	  var keyRange = Selector.newKeyRange(startKey, "", 101) 
	  var hasMore = true
	  while (hasMore) {
		  val columnMap = selector.getSuperColumnsFromRows(keyRange, "clustering", slicePredicate, ConsistencyLevel.ONE)
		  if(columnMap.size>0) {
		 	  //get the array of keys
  			  val columnsObj = List(columnMap.keySet.toArray : _*)
			  val columns = columnsObj.asInstanceOf[List[String]]
		 	  startKey = columns.last
		 	  val keys = columns.dropRight(1)
  			  //print last 
		 	  for(key<-keys){
		 	 	  val superColumn = columnMap.get(key).get(0)
		 	 	  val cellIds = superColToList(superColumn)
		 		  proc(key, cellIds)
		 	  }  
		 		  
			  keyRange = Selector.newKeyRange(startKey, "", 101)
		  } 
		  
		  if(columnMap.size<100){
		 	  hasMore = false
		  }
	  }
  }
  
  def pearsonCorrelation(x:List[(String,String)], y:List[(String,String)]) : Double = {
	  
	  //create a list of x values for all the (x,y) tuples
	  
	  
	  
	  //find the cell id matches
	  var matches = 0
	  var misses = 0
	  for(xn <- x){
		  if(y.contains(xn)){
		 	  matches+=1
		  } else {
		 	  misses-=1
		  }
	  }
	  
	  for(yn<-y){
		  if(!x.contains(yn)){
		 	  misses-=1
		  }
	  }
	   
	  if(matches==0){
	 	  return 0
	  }
	  
	  val sum1 = matches
	  val sum2 = matches

	  val sum1Sq = Math.pow(matches,2)
	  val sum2Sq = Math.pow(matches,2)

	  val pSum = matches * matches
	  
	  val num = pSum -(sum1*sum2/matches)
	  val den = Math.sqrt((sum1Sq - Math.pow(sum1,2)/matches) * (sum2Sq-Math.pow(sum2,2)/matches)) 
	  if(den==0){
	 	  return 0
	  }
	  num/den
  }
}

object Load extends Application {
  val hosts = Array{"localhost"}
  Pelops.addPool("test-pool",hosts, 9160, false, "occurrence", new Policy());
  val selector = Pelops.createSelector("test-pool", "occurrence");
  val slicePredicate = new SlicePredicate
  val columnNames = new ArrayList[Array[Byte]]
  columnNames.add("cells".getBytes)
  slicePredicate.setColumn_names(columnNames)
  
  val loader = new LoadPearsonCorrelation
  
  //iterate through all rows
  loader.pageOverAll(selector, slicePredicate, (keyToTest, listToTest) => {
	  //iterate through all rows - and compare against this row
	  loader.pageOverAll(selector, slicePredicate, (key,list) => {
		  if(!list.isEmpty){
		 	  print("keyToTest: " + keyToTest)
		 	  print(", key: " + key)
		 	  print(", pearson: " + loader.pearsonCorrelation(listToTest, list))
//			  print(", "+list)
			  print("\n")
		  }
	  })
  })

  println("finished")
  Pelops.shutdown
}
