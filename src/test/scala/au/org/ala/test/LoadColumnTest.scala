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

object LoadColumnTest {
	def main(args : Array[String]) : Unit = {
		println("Super column test starting")
		val hosts = Array{"localhost"}
		val sessions = new CassandraSessionPool("occurrence",StackPool(SocketProvider("localhost", 9160)),Protocol.Binary,ConsistencyLevel.ONE)
		val session = sessions.newSession
		loadRow("dr359", 8000000, session)
		session.close
	}
	
	def loadRow(key:String, noOfColumns:Int, session:CassandraSession){
		print("loading: "+key+", with columns: "+noOfColumns)
		val start = System.currentTimeMillis
		val rawPath = new ColumnPath("dr")
		for(i <- 0 until noOfColumns){
			val recordUuid = UUID.randomUUID.toString
			session ++| (key, rawPath.setColumn(recordUuid.getBytes), "1".getBytes, System.currentTimeMillis)
			session.flush
		}
		val finish = System.currentTimeMillis
		print(", Time taken (secs) :" +((finish-start)/1000) + " seconds.\n")
	}
}