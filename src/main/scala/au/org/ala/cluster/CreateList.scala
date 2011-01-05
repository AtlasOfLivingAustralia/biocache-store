package au.org.ala.cluster

import java.io._
import scala.collection.mutable.LinkedList

/**
 * select species_concept_id, cell_id from occurrence_record 
 * where species_concept_id is not null and cell_id is not null
 * group by species_concept_id, cell_id
 * order by species_concept_id, cell_id
 * into outfile '/tmp/oc.txt'
 */
object CreateList extends Application {
  
  import au.org.ala.util.FileHelper._

  println("running")

  val occurrenceCells = new File("/data/clustering/oc.txt")
  val occurrenceWriter = new OutputStreamWriter(new FileOutputStream("/data/clustering/oc-list.txt"))

  //read rows until row name changes  
  val br = new BufferedReader(new FileReader(occurrenceCells))
  var rowName = ""
  var values = new scala.collection.mutable.LinkedList[String]
  
  try { 
	  while(br.ready){ 
		  val parts = br.readLine.split("\t")
		  if(rowName == "" || rowName != parts(0)){

		 	if(values.nonEmpty && values.size > 20){
			    //write the map
			    occurrenceWriter.write(rowName)
			    for(value <- values){
			      occurrenceWriter.write('\t')
			      occurrenceWriter.write(value)
			    }
			    occurrenceWriter.write('\n')
		 	}
		    //reset variables
		    values = new scala.collection.mutable.LinkedList[String]
		    rowName = parts(0)
		  }
		   
	      values = values :+ parts(1)
	  }
  }
  finally { br.close }

  occurrenceWriter.flush
  occurrenceWriter.close
  
  println("finished")
}