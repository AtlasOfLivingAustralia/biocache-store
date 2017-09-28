package au.org.ala.biocache.persistence

/**
 * Trait (interface) for persistence storage in the Biocache.
 * 
 * This trait is implemented for Cassandra,
 * but could also be implemented for another backend supporting basic key value pair storage and
 * allowing the selection of a set of key value pairs via a rowkey.
 *
 * The rowkey is the primary key for the entity.
 *
 * @author Dave Martin (David.Martin@csiro.au)
 */
trait PersistenceManager {

  def rowKeyExists(rowKey:String, entityName:String) : Boolean

  /**
   * Get a single property.
   */
  def get(rowkey:String, entityName:String, propertyName:String) : Option[String]

  /**
   * Gets the supplied properties for this record
   */
  def getSelected(rowkey:String, entityName:String, propertyNames:Seq[String]):Option[Map[String,String]]

  /**
   * Get a key value pair map for this record.
   */
  def get(rowkey:String, entityName:String): Option[Map[String, String]]

  /**
   * Get a key value pair map for this column timestamps of this record.
   */
  def getColumnsWithTimestamps(rowkey:String, entityName:String): Option[Map[String, Long]]

  /**
   * Gets KVP map for a record based on a value in an index
   */
  def getByIndex(rowkey:String, entityName:String, idxColumn:String) : Option[Map[String,String]]

  /**
   * Gets a single property based on an indexed value.  Returns the value of the "first" matched record.
   */
  def getByIndex(rowkey:String, entityName:String, idxColumn:String, propertyName:String) :Option[String]

  /**
   * Retrieve an array of objects from a single column.
   */
  def getList[A](rowkey: String, entityName: String, propertyName: String, theClass:java.lang.Class[_]) : List[A]

  /**
    * Put a single property.
    */
  def put(rowkey: String, entityName: String, propertyName: String, propertyValue: String, newRecord:Boolean, deleteIfNullValue: Boolean): String

  /**
   * Put a set of key value pairs.
   */
  def put(rowkey: String, entityName: String, keyValuePairs: Map[String, String], newRecord:Boolean, removeNullFields: Boolean): String

  /**
   * Add a batch of properties.
   */
  def putBatch(entityName: String, batch: Map[String, Map[String, String]], newRecord:Boolean, removeNullFields: Boolean)

  /**
   * Store a list of the supplied object
   * @param overwrite if true, current stored value will be replaced without a read.
   */
  def putList[A](rowkey: String, entityName: String, propertyName: String, objectList:Seq[A], theClass:java.lang.Class[_], newRecord:Boolean, overwrite: Boolean, deleteIfNullValue: Boolean) : String

  /**
   * Page over all entities, passing the retrieved rowkey and property map to the supplied function.
   * Function should return false to exit paging.
   */
  def pageOverAll(entityName:String, proc:((String, Map[String,String])=>Boolean), startRowkey:String="", endRowkey:String="", pageSize:Int = 1000)

  /**
   * Page over all records using an indexed field
   *
   * @param entityName
   * @param proc
   * @param indexedField
   * @param indexedFieldValue
   * @param threads
   */
  def pageOverIndexedField(entityName:String, proc:((String, Map[String, String]) => Boolean), indexedField:String="", indexedFieldValue:String = "", threads:Int = 1, localOnly:Boolean = true) : Int

  /**
   * Page over the records that are local to this node.
   * @param entityName
   * @param proc
   * @param threads
   * @return
   */
  def pageOverLocal(entityName:String, proc:((String, Map[String, String], String) => Boolean), threads:Int, columns:Array[String]) : Int

  /**
   * Page over the records, retrieving the supplied columns only.
   */
  def pageOverSelect(entityName:String, proc:((String, Map[String,String])=>Boolean), indexedField:String, indexedFieldValue:String, pageSize:Int, columnName:String*) : Int

  /**
   * Page over the records, retrieving the supplied columns range.
   */
  def pageOverColumnRange(entityName:String, proc:((String, Map[String,String])=>Boolean), startRowkey:String="", endRowkey:String="", pageSize:Int=1000, startColumn:String="", endColumn:String="")

  /**
   * Whether range queries are supported by this persistence manager
   *
   * @return
   */
  def rangesSupported = false

  /**
   * Select the properties for the supplied record rowkeys
   */
  def selectRows(rowkeys:Seq[String], entityName:String, propertyNames:Seq[String], proc:((Map[String,String])=>Unit))

  /**
   * The column to delete.
   */
  def deleteColumns(rowKey:String, entityName:String, columnName:String*)

  /**
   * Delete row
   */
  def delete(rowKey:String, entityName:String)

  /**
   * Close db connections etc
   */
  def shutdown

  /**
   * The field delimiter to use
   */
  def fieldDelimiter = '.'

  /**
   * The field delimiter to use
   */
  def caseInsensitiveFields = false

  /**
    *
    * @param entityName
    * @param indexFieldName
    * @param threads
    * @return
    */
  def createSecondaryIndex(entityName:String, indexFieldName:String, threads:Int) : Int
}



