package au.org.ala.cluster

import java.io._
import scala.collection.mutable.LinkedList
import scala.Application
import se.scalablesolutions.akka.persistence.cassandra._
import se.scalablesolutions.akka.persistence.common._
import org.apache.cassandra.thrift.ConsistencyLevel
import org.apache.cassandra.thrift.ColumnPath
import org.apache.thrift.transport.TTransport
import scala.reflect._
import java.io._

object LoadSpeciesTable {
  def main(args : Array[String]) : Unit = {
	  
  import au.org.ala.util.FileHelper._

  println("running")

  //read rows until row name changes
  val occurrenceCells = new File("/data/clustering/species-centi_cell.txt")
  val br = new BufferedReader(new FileReader(occurrenceCells))
  var values = new scala.collection.mutable.LinkedList[String]
  val sessions = new CassandraSessionPool("occurrence",StackPool(SocketProvider("localhost", 9160)),Protocol.Binary,ConsistencyLevel.ONE)
  val session = sessions.newSession
  val time = System.currentTimeMillis
  try { 
	  while(br.ready){ 
  	    val parts = br.readLine.split("\t")
	    val speciesId = parts(0)
		val centiCellId = parts(1)
		val count = parts(2)
		//write to cassandra
		val rawPath = new ColumnPath("clustering")
  	    rawPath.setSuper_column("cells".getBytes);
  	    session ++| (speciesId, rawPath.setColumn(centiCellId.getBytes), count.getBytes, time)
	  }
  }
  finally { br.close }
  	println("finished load")
  }
}
