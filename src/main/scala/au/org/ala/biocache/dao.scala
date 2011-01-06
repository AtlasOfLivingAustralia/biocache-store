package au.org.ala.biocache

import com.google.gson.reflect.TypeToken
import com.google.gson.Gson
import au.org.ala.util.ReflectBean
import scala.reflect.BeanProperty
import org.wyki.cassandra.pelops.{Mutator,Pelops,Policy,Selector}
import scala.collection.mutable.{LinkedList,ListBuffer}
import org.apache.cassandra.thrift.{Column,ConsistencyLevel,ColumnPath,SlicePredicate,SliceRange}
import java.util.ArrayList

object DAO {
	val hosts = Array{"localhost"}
	val keyspace = "occurrence"
	val poolName = "test-pool"
	Pelops.addPool(poolName, hosts, 9160, false, keyspace, new Policy())
	//read in the ORM mappings
	val occurrenceDefn = scala.io.Source.fromURL(DAO.getClass.getResource("/Occurrence.txt"), "utf-8").getLines.toList.map(_.trim).toArray
	val locationDefn = scala.io.Source.fromURL(DAO.getClass.getResource("/Location.txt"), "utf-8").getLines.toList.map(_.trim).toArray
	val eventDefn = scala.io.Source.fromURL(DAO.getClass.getResource("/Event.txt"), "utf-8").getLines.toList.map(_.trim).toArray
	val classificationDefn = scala.io.Source.fromURL(DAO.getClass.getResource("/Classification.txt"), "utf-8").getLines.toList.map(_.trim).toArray
	val identificationDefn = scala.io.Source.fromURL(DAO.getClass.getResource("/Identification.txt"), "utf-8").getLines.toList.map(_.trim).toArray
}

class LocationDAO {

	val columnFamily = "location"

	def addTagToLocation (latitude:Float, longitude:Float, tagName:String, tagValue:String) {
		val guid = latitude +"|"+longitude
    	val mutator = Pelops.createMutator(DAO.poolName, DAO.keyspace)
    	mutator.writeColumn(guid, "location", mutator.newColumn("decimalLatitude", latitude.toString))
    	mutator.writeColumn(guid, "location", mutator.newColumn("decimalLongitude", longitude.toString))
		mutator.writeColumn(guid, "location", mutator.newColumn(tagName, tagValue)) 
		mutator.execute(ConsistencyLevel.ONE)
	}

	def addRegionToPoint (latitude:Float, longitude:Float, mapping:Map[String,String]) {
		val guid = latitude +"|"+longitude
    	val mutator = Pelops.createMutator(DAO.poolName, DAO.keyspace)
    	mutator.writeColumn(guid, "location", mutator.newColumn("decimalLatitude", latitude.toString))
    	mutator.writeColumn(guid, "location", mutator.newColumn("decimalLongitude", longitude.toString))
    	for(map<-mapping){
    		mutator.writeColumn(guid, "location", mutator.newColumn(map._1, map._2))
    	}
		mutator.execute(ConsistencyLevel.ONE)
	}
	
	def getLocationByLatLon(latitude:String, longitude:String) : Option[Location] = {
		try {
			val uuid = latitude+"|"+longitude
			//println(uuid)
			val selector = Pelops.createSelector(DAO.poolName, DAO.keyspace)
			val slicePredicate = new SlicePredicate
			val sliceRange = new SliceRange
			sliceRange.setStart("".getBytes)
			sliceRange.setFinish("".getBytes)
			slicePredicate.setSlice_range(sliceRange)
			
			val columns = selector.getColumnsFromRow(uuid, columnFamily, slicePredicate, ConsistencyLevel.ONE)
			val columnList = List(columns.toArray : _*)
			val location = new Location
			for(column<-columnList){
				val field = new String(column.asInstanceOf[Column].name)
				val value = new String(column.asInstanceOf[Column].value)
				//println(new String(column.asInstanceOf[Column].name)+ " " +column.asInstanceOf[Column].value)
				//println("field name : " + field+", value : "+value)
				val method = location.getClass.getMethods.find(_.getName == field + "_$eq")
				method.get.invoke(location, value.asInstanceOf[AnyRef])
			}
			Some(location)
		} catch {
			case e:Exception => println(e.printStackTrace); None
		}
	}
}

/**
 * A DAO for accessing occurrences.
 * 
 * @author Dave Martin (David.Martin@csiro.au)
 */
class OccurrenceDAO {

	import OccurrenceType._
	import ReflectBean._
	
	val columnFamily = "occurrence"

	/**
	 * Get an occurrence with UUID
	 * 
	 * @param uuid
	 * @return
	 */
	def getByUuid(uuid:String) : Option[(Occurrence, Classification, Location, Event)] = {
		getByUuid(uuid, OccurrenceType.Raw)
	}

	/**
	 * Get an occurrence, specifying the version of the occurrence.
	 * 
	 * @param uuid
	 * @param occurrenceType
	 * @return
	 */
	def getByUuid(uuid:String, occurrenceType:OccurrenceType.Value) : Option[(Occurrence, Classification, Location, Event)] = {
		
		val selector = Pelops.createSelector(DAO.poolName, DAO.keyspace)
		val slicePredicate = new SlicePredicate
		val sliceRange = new SliceRange
		//retrieve all columns
		sliceRange.setStart("".getBytes)
		sliceRange.setFinish("".getBytes)
		slicePredicate.setSlice_range(sliceRange)
		
		val occurrence = new Occurrence
		val columnList = selector.getColumnsFromRow(uuid, columnFamily, slicePredicate, ConsistencyLevel.ONE)
		createOccurrence(uuid, columnList, occurrenceType)
	}
	
	/**
	 * Set the property on the correct model object
	 * @param o the occurrence
	 * @param c the classification
	 * @param l the location
	 * @param e the event
	 * @param fieldName the field to set
	 * @param fieldValue the value to set
	 */
	def setProperty(o:Occurrence, c:Classification, l:Location, e:Event, fieldName:String, fieldValue:String){
	  if(DAO.occurrenceDefn.contains(fieldName)){
	 	  o.setter(fieldName,fieldValue)
	  } else if(DAO.classificationDefn.contains(fieldName)){
	 	  c.setter(fieldName,fieldValue)
	  } else if(DAO.eventDefn.contains(fieldName)){
	 	  e.setter(fieldName,fieldValue)
	  } else if(DAO.locationDefn.contains(fieldName)){
	 	  l.setter(fieldName,fieldValue)
	  }
	}
	
	/**
	 * Creates an occurrence from the list of columns.
	 * An occurrence consists of several objects which are returned as a tuple. 
	 * 
	 * For a java implementation, a DTO containing the objects will need to be returned.
	 * 
	 * @param uuid
	 * @param columnList
	 * @param occurrenceType raw, processed or consensus version of the record
	 * @return
	 */
	def createOccurrence(uuid:String, columnList:java.util.List[Column], occurrenceType:OccurrenceType.Value) 
		: Option[(Occurrence, Classification, Location, Event)] = {
		
		val occurrence = new Occurrence
		val classification = new Classification
		val location = new Location
		val event = new Event
		
		occurrence.uuid = uuid
	  	val columns = List(columnList.toArray : _*)
 	    for(column<-columns){
 	 	
 	      //ascertain which term should be associated with which object
 		  var fieldName = new String(column.asInstanceOf[Column].name)
 		  val fieldValue = new String(column.asInstanceOf[Column].value)
 		  
 		  if(fieldName.endsWith(".p") && occurrenceType == OccurrenceType.Processed){
 		 	  fieldName = fieldName.substring(0, fieldName.length - 2)
 		 	  setProperty(occurrence, classification, location, event, fieldName, fieldValue)
 		  } else if(fieldName.endsWith(".c") && occurrenceType == OccurrenceType.Consensus){
 		 	  fieldName = fieldName.substring(0, fieldName.length - 2)
 		 	  setProperty(occurrence, classification, location, event, fieldName, fieldValue)
 		  } else {
 		 	  setProperty(occurrence, classification, location, event, fieldName, fieldValue)
 		  }
 	    }  
		Some((occurrence, classification, location, event))
	}
	
	/**
	 * Iterate over all occurrences, passing the objects to a function.
	 * 
	 * @param occurrenceType
	 * @param proc
	 */
	def pageOverAll(occurrenceType:OccurrenceType.Value, proc:((Option[(Occurrence, Classification, Location, Event)])=>Unit) ) : Unit = {
		
	  val selector = Pelops.createSelector(DAO.poolName, columnFamily);
	  val slicePredicate = new SlicePredicate
	  val sliceRange = new SliceRange
	  //blank key ranges to select all columns
	  sliceRange.setStart("".getBytes)
	  sliceRange.setFinish("".getBytes)
	  slicePredicate.setSlice_range(sliceRange)
		
	  var startKey = ""
	  var keyRange = Selector.newKeyRange(startKey, "", 101) 
	  var hasMore = true
	  var counter = 0
	  while (hasMore) {
		  val columnMap = selector.getColumnsFromRows(keyRange, columnFamily, slicePredicate, ConsistencyLevel.ONE)
		  if(columnMap.size>0) {
  			  val columnsObj = List(columnMap.keySet.toArray : _*)
			  val columns = columnsObj.asInstanceOf[List[String]]
		 	  startKey = columns.last
		 	  val keys = columns.dropRight(1)
		 	  for(key<-keys){
		 	 	  val columnsList = columnMap.get(key)
		 	 	  proc(createOccurrence(key, columnsList, occurrenceType))
		 	  }  
		 	  counter += columnMap.size -1
			  keyRange = Selector.newKeyRange(startKey, "", 101)
		  } 
		  if(columnMap.size<100){
		 	  hasMore = false
		  }
	  }
	  println("finished") 
    }
	
	/**
	 * Update an occurrence
	 * 
	 * @param uuid
	 * @param anObject
	 * @param occurrenceType
	 */
	def updateOccurrence(uuid:String, anObject:AnyRef, occurrenceType:OccurrenceType.Value) {
		
		//select the correct definition file
		var defn = DAO.occurrenceDefn
		if(anObject.isInstanceOf[Location]) defn = DAO.locationDefn
		else if(anObject.isInstanceOf[Event]) defn = DAO.eventDefn
		else if(anObject.isInstanceOf[Classification]) defn = DAO.classificationDefn
		//additional functionality to support adding Quality Assertions and Field corrections.
		
		val mutator = Pelops.createMutator(DAO.poolName, columnFamily);
		for(field <- defn){
			
			val fieldValue = anObject.getClass.getMethods.find(_.getName == field).get.invoke(anObject).asInstanceOf[String]
			if(fieldValue!=null && !fieldValue.isEmpty){
				var fieldName = field
				if(occurrenceType == OccurrenceType.Processed){
					fieldName = fieldName +".p"
				}
				if(occurrenceType == OccurrenceType.Consensus){
					fieldName = fieldName +".c"
				}
				mutator.writeColumn(uuid, columnFamily, mutator.newColumn(fieldName, fieldValue))
			}
		}
		mutator.execute(ConsistencyLevel.ONE)
	}

	def addFieldCorrection(uuid:String, fieldCorrection:FieldCorrection){
		
		//set field corrections.dwc.<field-name>
		//list of FieldCorrection objects ?????
			//store disagrees/agrees
	}	
	
	def addQualityAssertion(uuid:String, qualityAssertion:QualityAssertion){
		
		//set field qualityAssertion
		val selector = Pelops.createSelector(DAO.poolName, columnFamily);
		val mutator = Pelops.createMutator(DAO.poolName, columnFamily);
		val column = {
			try {
			 Some(selector.getColumnFromRow(uuid, columnFamily, "qualityAssertion".getBytes, ConsistencyLevel.ONE))
			} catch {
				case _ => None
			}
		}
		val gson = new Gson
		
		if(column.isEmpty){
			//parse it
			val json = gson.toJson(Array(qualityAssertion))
			mutator.writeColumn(uuid, columnFamily, mutator.newColumn("qualityAssertion", json))
		} else {
			var json = new String(column.get.getValue)
			val listType = new TypeToken[ArrayList[QualityAssertion]]() {}.getType()
			var qaList = gson.fromJson(json,listType).asInstanceOf[java.util.List[QualityAssertion]]
			
			var written = false
			for(i<- 0 until qaList.size){
				val qa = qaList.get(i)
				if(qa equals qualityAssertion){
					//overwrite
					written = true
					qaList.remove(qa)
					qaList.add(i, qualityAssertion)
				}
			}
			if(!written){
				qaList.add(qualityAssertion)
			}
			
			// check equals
			json = gson.toJson(qaList)
			mutator.writeColumn(uuid, columnFamily, mutator.newColumn("qualityAssertion", json))
		}
		mutator.execute(ConsistencyLevel.ONE)
	}
	
	def getQualityAssertions(uuid:String){
		
		
	}
	
	
	def addAnnotation(){
		
		//
		
	}
}
