package au.org.ala.biocache.util

import java.util.ArrayList

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.`type`.TypeFactory
import com.fasterxml.jackson.databind.{DeserializationConfig, ObjectMapper}

object Json {

  import scala.collection.JavaConverters._

  //object mapper is thread safe
  val mapper = new ObjectMapper
  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)

  def toJSONWithGeneric[A](list:Seq[A]) : String = {
    mapper.writeValueAsString(list.asJava)
  }  
  
  def toJSONWithGeneric[A](list:List[A]) : String = {
    mapper.writeValueAsString(list.asJava)
  }

  /**
   * Convert the supplied list to JSON
   */
  def toJSON(list:List[AnyRef]) : String = {
    mapper.writeValueAsString(list.asJava)
  }

  /**
   * Convert the supplied list to JSON
   */
  def toJSON(a:AnyRef) : String = {
    if(a.isInstanceOf[scala.collection.Map[AnyRef, AnyRef]]){
      mapper.writeValueAsString(a.asInstanceOf[scala.collection.Map[AnyRef, AnyRef]].asJava)
    } else {
      mapper.writeValueAsString(a)
    }
  }

  /**
   * Convert the supplied list to JSON
   */
  def toJSON(a:Map[String,Any]) : String = {
    mapper.writeValueAsString(a.asJava)
  }

  /**
   * Convert the supplied list to JSON
   */
  def toJSONMap(a:Map[String,Any]) : String = {
    mapper.writeValueAsString(a.asJava)
  }

  /**
   * Convert Array to JSON
   */
  def toJSON(arr:Array[AnyRef]) : String ={
    mapper.writeValueAsString(arr)
  }

  /**
   * Converts a string to the supplied array type
   */
  def toStringArray(jsonString:String) : Array[String] = {
    if(jsonString != null && jsonString != ""){
      val valueType = TypeFactory.defaultInstance().constructArrayType(classOf[java.lang.String])
      mapper.readValue[Array[String]](jsonString, valueType)
    } else {
      Array()
    }
  }

  /**
   * Converts a string to the supplied array type
   */
  def toArray(jsonString:String, theClass:java.lang.Class[AnyRef]) : Array[AnyRef] ={
    val valueType = TypeFactory.defaultInstance().constructArrayType(theClass)
    mapper.readValue[Array[AnyRef]](jsonString, valueType)
  }

  /**
   * Convert the supplied list from JSON
   */
  def toList(jsonString:String, theClass:java.lang.Class[AnyRef]) : List[AnyRef] = {

      val valueType = TypeFactory.defaultInstance().constructCollectionType(classOf[ArrayList[AnyRef]], theClass)
      val listOfObject = mapper.readValue[ArrayList[AnyRef]](jsonString, valueType)
      listOfObject.asScala.toList
  }

  /**
   * Convert the supplied list from JSON
   */
  def toListWithGeneric[A](jsonString:String,theClass:java.lang.Class[_]) : List[A] = {
    val valueType = mapper.getTypeFactory.constructCollectionType(classOf[ArrayList[_]], theClass)
    val listOfObject = mapper.readValue[ArrayList[_]](jsonString, valueType)
    listOfObject.asScala.toList.asInstanceOf[List[A]]
  }

  /**
   * Convert the supplied list from JSON
   */
  def toIntArray(jsonString:String) : Array[Int] = {
    if (jsonString =="" || jsonString =="[]") return Array()
    jsonString.replace("[","").replace("]","").split(",").map(x => x.toInt).toArray
  }

  def toMap(jsonString:String): scala.collection.Map[String,Object]={
   try {
     mapper.readValue(jsonString,classOf[java.util.Map[String,Object]]).asScala
   } catch {
     case e:Exception => Map()
   }
  }

  def toStringMap(jsonString:String): scala.collection.Map[String,String] = {
   try {
     mapper.readValue(jsonString,classOf[java.util.Map[String,String]]).asScala
   } catch {
     case e:Exception => Map()
   }
  }

  def toJavaMap(jsonString:String): java.util.Map[String,Object] = {
   try {
     mapper.readValue(jsonString,classOf[java.util.Map[String,Object]])
   } catch {
     case e:Exception => new java.util.HashMap()
   }
  }

  def toJavaStringMap(jsonString:String): java.util.Map[String,String] = {
   try {
     mapper.readValue(jsonString,classOf[java.util.Map[String,String]])
   } catch {
     case e:Exception => new java.util.HashMap()
   }
  }
}