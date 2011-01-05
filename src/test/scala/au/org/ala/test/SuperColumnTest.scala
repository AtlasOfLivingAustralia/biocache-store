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
import org.wyki.cassandra.pelops.Mutator
import org.wyki.cassandra.pelops.Pelops
import org.wyki.cassandra.pelops.Policy
import org.wyki.cassandra.pelops.Selector
import scala.collection.immutable.Set
import scala.collection.mutable.ListBuffer
import se.scalablesolutions.akka.persistence.cassandra._
import se.scalablesolutions.akka.persistence.common._
import org.apache.cassandra.thrift.ConsistencyLevel
import org.apache.cassandra.thrift.ColumnPath
import org.apache.thrift.transport.TTransport
import au.org.ala.cluster._
import java.util.UUID


object SuperColumnTest {
	def main(args : Array[String]) : Unit = {
		
		import au.org.ala.util.FileHelper._
		
		println("Super column test starting")
		
		//load up data resources
		val hosts = Array{"localhost"}
		Pelops.addPool("test-pool",hosts, 9160, false, "occurrence", new Policy());
		val selector = Pelops.createSelector("test-pool", "occurrence");
		val sessions = new CassandraSessionPool("occurrence",StackPool(SocketProvider("localhost", 9160)),Protocol.Binary,ConsistencyLevel.ONE)
		val session = sessions.newSession
		
		//write to cassandra
		val file = new File("/data/dr.txt")
//		file.foreachLine { line => loadDR(line, session) }
		loadDR("dr359\t8000000", session)
		session.close
	}
	
	def loadDR(line:String, session:CassandraSession){
		val parts = line.split('\t')
		val uuid = parts(0)
		val noOfColumns = Integer.parseInt(parts(1))
		
		print("loading: "+uuid+", with columns: "+noOfColumns)
		val start = System.currentTimeMillis
		val rawPath = new ColumnPath("dr")
		for(i <- 0 until noOfColumns){
			val recordUuid = UUID.randomUUID.toString
			session ++| (uuid, rawPath.setColumn(recordUuid.getBytes), "1".getBytes, System.currentTimeMillis)
			session.flush
		}
		val finish = System.currentTimeMillis
		print(", Time taken (secs) :" +((finish-start)/1000) + " seconds.\n")
	}
}