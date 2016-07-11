package au.org.ala.biocache.poso

import scala.util.parsing.json.JSON
import scala.collection.immutable.Map
import au.org.ala.biocache.util.Json

/**
 * A POSO with nested POSOs
 */
trait CompositePOSO extends POSO {

  private val posoGetterLookup = ReflectionCache.getCompositeLookup(this)
  private val nestedProperties = posoGetterLookup.keys

  override def hasProperty(name: String) = (!lookupProperty(name).isEmpty || !posoGetterLookup.get(name.toLowerCase).isEmpty)

  def hasNestedProperty(name: String) = !posoGetterLookup.get(name.toLowerCase).isEmpty

  /**
   * Properties for this composite POSO lower cased.
   * @return
   */
  override def getPropertyNames: List[String] = ReflectionCache.getCompositePropertyNames(this)

  override def setProperty(name: String, value: String) : Boolean = {

    var success = true
    lookupProperty(name) match {

      case Some(property) => {
        if (property.typeName == "scala.collection.immutable.Map") {
          val jsonOption = JSON.parseFull(value)
          if (!jsonOption.isEmpty) {
            try {
              property.setter.invoke(this, jsonOption.get.asInstanceOf[Map[String, String]])
            } catch {
              case e: Exception => {
                success = false
                logger.error(e.getMessage, e)
              }
            }
          }
        } else if (property.typeName == "[Ljava.lang.String;") {
          val jsonOption = JSON.parseFull(value)
          if (!jsonOption.isEmpty && jsonOption.get.isInstanceOf[Array[String]]) {
            try {
              val stringArray = jsonOption.get.asInstanceOf[Array[String]]
              if (!stringArray.isEmpty) {
                property.setter.invoke(this, jsonOption.get.asInstanceOf[Array[String]])
              }
            } catch {
              case e: Exception => {
                success = false
                logger.error(e.getMessage, e)
              }
            }
          }
        } else if (property.typeName == "java.util.Map") {
          val stringMap = Json.toJavaStringMap(value)
          if (!stringMap.isEmpty) {
            try {
              property.setter.invoke(this, stringMap)
            } catch {
              case e: Exception => {
                success = false
                logger.error(e.getMessage, e)
              }
            }
          }
        } else {
          //NC The print statement below can be enabled for debug purposes but please don't
          //check it back without commenting it out because it creates a lot of output when loading a DwcA
          //println(property.name + " : "+property.typeName + " : " + value)
          property.setter.invoke(this, value)
        }
      }
      case None => setNestedProperty(name, value)
    }
    success
  }

  override def getProperty(name: String): Option[String] = lookupProperty(name) match {
    case Some(property) => Some(property.getter.invoke(this).toString)
    case None => getNestedProperty(name)
  }

  def getNestedProperty(name: String): Option[String] = {
    val getter = posoGetterLookup.get(name)
    getter match {
      case Some(method) => {
        val poso = method.invoke(this).asInstanceOf[POSO]
        poso.getProperty(name)
      }
      case None => None
    }
  }

  def setNestedProperty(name: String, value: String) {
    val getter = posoGetterLookup.get(name.toLowerCase)
    getter match {
      case Some(method) => {
        val poso = method.invoke(this).asInstanceOf[POSO]
        poso.setProperty(name, value)
      }
      case None => //do nothing
    }
  }

  def getNestedProperties(): Iterable[String] = {
    nestedProperties
  }
}