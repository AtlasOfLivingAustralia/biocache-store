package au.org.ala.cluster

import java.io._
import scala.collection._
import scala.collection.mutable._

object CreateMatrix extends Application {
  
  import au.org.ala.util.FileHelper._

  println("running")

  val occurrenceCells = new File("/tmp/oc-small.txt")
  val occurrenceMatrix = new OutputStreamWriter(new FileOutputStream("/tmp/oc-matrix.txt"))

  var rows = new scala.collection.mutable.LinkedHashSet[String];
  var cols = new scala.collection.mutable.LinkedHashSet[String];

  //read the file and get the columns headers and row names
  occurrenceCells.foreachLine{ line => storeXandY(line, rows, cols) }

  println("Rows (species) " + rows.size)
  println("Columns (centi cells) " + cols.size)

  //read the file and get the row headers
  
  //write column headers
  occurrenceMatrix.write('\t')
  for (col <- cols) {
    occurrenceMatrix.write(col)
    occurrenceMatrix.write('\t')
  }
  occurrenceMatrix.write('\n')
  
  //read rows until row name changes  
  val br = new BufferedReader(new FileReader(occurrenceCells))
  var rowName = ""
  var values = new scala.collection.mutable.LinkedHashMap[String, String]
  
  try{ 
	  while(br.ready){ 
		  val parts = br.readLine.split("\t")
		  if(rowName == "" || rowName!=parts(0)){
			
//		    println(rowName)
		    //handle the map
		    for(col <- cols){
		      val occurrenceVal = values.get(col)
		      occurrenceMatrix.write(occurrenceVal.getOrElse("0"))
		      occurrenceMatrix.write('\t')
		    }
		    
		    //reset column name
		    rowName = parts(0)
		    occurrenceMatrix.write('\n')
		    occurrenceMatrix.write(rowName)
		    occurrenceMatrix.write('\t')
		    values.clear
		  } 
		  
	      values.put(parts(1), parts(2))
	  }
  }
  finally{ br.close }

  occurrenceMatrix.flush
  occurrenceMatrix.close
  
  println("finished")
  
  
  def handleRow(line:String, output:Writer, cols:scala.collection.mutable.Set[String]) : Unit ={
    
    
    
    
  }
  
  
  /**
   * return column header and row header
   */
  def storeXandY(line:String, rows:scala.collection.mutable.Set[String], cols:scala.collection.mutable.Set[String]) : Unit = {
    val parts = line.split("\t")
    rows + parts(0)
    cols + parts(1)
  }
}
