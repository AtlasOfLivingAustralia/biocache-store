package au.org.ala.biocache

import au.org.ala.checklist.lucene.CBIndexSearch
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
	val keyspace = "occ"
	val poolName = "occ-pool"
	val nameIndex= new CBIndexSearch("/data/lucene/namematching")
		
	Pelops.addPool(poolName, hosts, 9160, false, keyspace, new Policy())
	//read in the ORM mappings
	val attributionDefn = scala.io.Source.fromURL(DAO.getClass.getResource("/Attribution.txt"), "utf-8").getLines.toList.map(_.trim).toArray
	val occurrenceDefn = scala.io.Source.fromURL(DAO.getClass.getResource("/Occurrence.txt"), "utf-8").getLines.toList.map(_.trim).toArray
	val locationDefn = scala.io.Source.fromURL(DAO.getClass.getResource("/Location.txt"), "utf-8").getLines.toList.map(_.trim).toArray
	val eventDefn = scala.io.Source.fromURL(DAO.getClass.getResource("/Event.txt"), "utf-8").getLines.toList.map(_.trim).toArray
	val classificationDefn = scala.io.Source.fromURL(DAO.getClass.getResource("/Classification.txt"), "utf-8").getLines.toList.map(_.trim).toArray
	val identificationDefn = scala.io.Source.fromURL(DAO.getClass.getResource("/Identification.txt"), "utf-8").getLines.toList.map(_.trim).toArray
}

class AttributionDAO {

	import ReflectBean._
	val columnFamily = "attr"
	
	def addCollectionMapping(institutionCode:String, collectionCode:String, attribution:Attribution){
		val guid = institutionCode.toUpperCase +"|"+collectionCode.toUpperCase
    	val mutator = Pelops.createMutator(DAO.poolName, DAO.keyspace)
    	for(field<-DAO.attributionDefn){
    		val fieldValue = attribution.getter(field).asInstanceOf[String]
    		if(fieldValue!=null && !fieldValue.isEmpty){
	    		val fieldValue = attribution.getter(field).asInstanceOf[String].getBytes
	    		mutator.writeColumn(guid, columnFamily, mutator.newColumn(field, fieldValue))
    		}
    	}
		mutator.execute(ConsistencyLevel.ONE)
	}
	
	def getAttibutionByCodes(institutionCode:String, collectionCode:String) : Option[Attribution] = {
		try {
			if(institutionCode!=null && collectionCode!=null){
				val uuid = institutionCode.toUpperCase+"|"+collectionCode.toUpperCase
				//println(uuid)
				val selector = Pelops.createSelector(DAO.poolName, DAO.keyspace)
				val slicePredicate = Selector.newColumnsPredicateAll(true, 10000)
				val columns = selector.getColumnsFromRow(uuid, columnFamily, slicePredicate, ConsistencyLevel.ONE)
				val columnList = List(columns.toArray : _*)
				val attribution = new Attribution
				for(column<-columnList){
					val field = new String(column.asInstanceOf[Column].name)
					val value = new String(column.asInstanceOf[Column].value)
					val method = attribution.getClass.getMethods.find(_.getName == field + "_$eq")
					method.get.invoke(attribution, value.asInstanceOf[AnyRef])
				}
				Some(attribution)
			} else {
				None
			}
		} catch {
			case e:Exception => println(e.printStackTrace); None
		}
	}
}

class LocationDAO {

	val columnFamily = "loc"

	def addTagToLocation (latitude:Float, longitude:Float, tagName:String, tagValue:String) {
		val guid = latitude +"|"+longitude
    	val mutator = Pelops.createMutator(DAO.poolName, DAO.keyspace)
    	mutator.writeColumn(guid, columnFamily, mutator.newColumn("decimalLatitude", latitude.toString))
    	mutator.writeColumn(guid, columnFamily, mutator.newColumn("decimalLongitude", longitude.toString))
		mutator.writeColumn(guid, columnFamily, mutator.newColumn(tagName, tagValue)) 
		mutator.execute(ConsistencyLevel.ONE)
	}

	def addRegionToPoint (latitude:Float, longitude:Float, mapping:Map[String,String]) {
		val guid = latitude +"|"+longitude
    	val mutator = Pelops.createMutator(DAO.poolName, DAO.keyspace)
    	mutator.writeColumn(guid,columnFamily, mutator.newColumn("decimalLatitude", latitude.toString))
    	mutator.writeColumn(guid,columnFamily, mutator.newColumn("decimalLongitude", longitude.toString))
    	for(map<-mapping){
    		mutator.writeColumn(guid, columnFamily, mutator.newColumn(map._1, map._2))
    	}
		mutator.execute(ConsistencyLevel.ONE)
	}
	
	def getLocationByLatLon(latitude:String, longitude:String) : Option[Location] = {
		try {
			val uuid = latitude+"|"+longitude
			//println(uuid)
			val selector = Pelops.createSelector(DAO.poolName, DAO.keyspace)
			val slicePredicate = Selector.newColumnsPredicateAll(true, 10000)
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
	
	val columnFamily = "occ"

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
		val slicePredicate = Selector.newColumnsPredicateAll(true, 10000)
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
	  val slicePredicate = Selector.newColumnsPredicateAll(true, 10000);
	  var startKey = ""
	  var keyRange = Selector.newKeyRange(startKey, "", 1001) 
	  var hasMore = true
	  var counter = 0
	  var columnMap = selector.getColumnsFromRows(keyRange, columnFamily, slicePredicate, ConsistencyLevel.ONE)
	  while (columnMap.size>0) {
		  val columnsObj = List(columnMap.keySet.toArray : _*)
		  //convert to scala List
		  val keys = columnsObj.asInstanceOf[List[String]]
	 	  startKey = keys.last
	 	  for(key<-keys){
	 	 	  val columnsList = columnMap.get(key)
	 	 	  proc(createOccurrence(key, columnsList, occurrenceType))
	 	  }  
	 	  counter += keys.size
		  keyRange = Selector.newKeyRange(startKey, "", 1001)
		  columnMap = selector.getColumnsFromRows(keyRange, columnFamily, slicePredicate, ConsistencyLevel.ONE)
		  columnMap.remove(startKey)
	  }
	  println("Finished paging. Total count: "+counter) 
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
		var defn:Array[String] = null
		if(anObject.isInstanceOf[Location]) defn = DAO.locationDefn
		else if(anObject.isInstanceOf[Occurrence]) defn = DAO.occurrenceDefn
		else if(anObject.isInstanceOf[Event]) defn = DAO.eventDefn
		else if(anObject.isInstanceOf[Classification]) defn = DAO.classificationDefn
		else if(anObject.isInstanceOf[Attribution]) defn = DAO.attributionDefn
		else throw new RuntimeException("Unmapped object type: "+anObject)
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
