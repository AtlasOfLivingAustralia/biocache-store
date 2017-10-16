package au.org.ala.biocache.model

import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.poso.{CompositePOSO, POSO}
import au.org.ala.biocache.util.Json
import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.commons.lang.builder.EqualsBuilder
import org.codehaus.jackson.annotate.JsonIgnoreProperties

import scala.beans.BeanProperty

/**
  * Encapsulates a complete specimen or occurrence record.
  * TODO Split into two types, moving the processed bits into a subtype "ProcessedFullRecord".
  * This should make it clearer the what the differences are between a
  * raw and processed record.
  */
@JsonIgnoreProperties(Array("propertyNames"))
class FullRecord(
                  @BeanProperty var rowKey: String,
                  @BeanProperty var uuid: String,
                  @BeanProperty var occurrence: Occurrence,
                  @BeanProperty var classification: Classification,
                  @BeanProperty var location: Location,
                  @BeanProperty var event: Event,
                  @BeanProperty var attribution: Attribution,
                  @BeanProperty var identification: Identification,
                  @BeanProperty var measurement: Measurement,
                  @BeanProperty var assertions: Array[String] = Array(),
                  @BeanProperty var el: java.util.Map[String, String] = new java.util.HashMap[String, String](), //environmental layers
                  @BeanProperty var cl: java.util.Map[String, String] = new java.util.HashMap[String, String](), //contextual layers
                  @BeanProperty var miscProperties: java.util.Map[String, String] = new java.util.HashMap[String, String](),
                  @BeanProperty var queryAssertions: java.util.Map[String, String] = new java.util.HashMap[String, String](),
                  @BeanProperty var userQualityAssertion: String = "",
                  @BeanProperty var userAssertionStatus: String = "",
                  @BeanProperty var locationDetermined: Boolean = false,
                  @BeanProperty var defaultValuesUsed: Boolean = false,
                  @BeanProperty var geospatiallyKosher: Boolean = true,
                  @BeanProperty var taxonomicallyKosher: Boolean = true,
                  @BeanProperty var deleted: Boolean = false,
                  @BeanProperty var userVerified: Boolean = false,
                  @BeanProperty var firstLoaded: String = "",
                  @BeanProperty var lastModifiedTime: String = "",
                  @BeanProperty var dateDeleted: String = "",
                  @BeanProperty var lastUserAssertionDate: String = "",
                  @JsonIgnoreProperties var rawFields: scala.collection.Map[String, String] = Map(),
                  @JsonIgnoreProperties var qualityAssertions: scala.collection.Map[Int, QualityAssertion] = null)

  extends Cloneable with CompositePOSO {

  def findAssertions(codes: Array[Int] = null): Array[QualityAssertion] = {
    //build map on first request
    if (qualityAssertions == null) {
      val json = rawFields.get(FullRecordMapper.qualityAssertionColumn)
      var qaMap: scala.collection.Map[Int, QualityAssertion] = Map()
      if (!json.isEmpty) {
        Json.toListWithGeneric(json.get, classOf[QualityAssertion]).asInstanceOf[List[QualityAssertion]].foreach { qa =>
          qaMap += (qa.code -> qa)
        }
      }
      qualityAssertions = qaMap
    }

    if (codes == null) {
      qualityAssertions.values.toArray
    } else {
      qualityAssertions.filter { qa =>
        codes.contains(qa._1)
      }.values.toArray
    }
  }

  def objectArray: Array[POSO] = Array(occurrence, classification, location, event, attribution, identification, measurement)

  def this(rowKey: String, uuid: String) = this(rowKey, uuid, new Occurrence, new Classification, new Location, new Event, new Attribution, new Identification,
    new Measurement)

  def this() = this(null, null, new Occurrence, new Classification, new Location, new Event, new Attribution, new Identification,
    new Measurement)

  /**
    * Creates an empty new Full record based on this one to be used in Processing.
    * Initialises the userVerified and ids for use in processing
    */
  def createNewProcessedRecord: FullRecord = {
    val record = new FullRecord(this.rowKey, this.uuid)
    record.userVerified = this.userVerified
    record
  }

  override def clone: FullRecord = new FullRecord(
    this.rowKey,
    this.uuid,
    occurrence.clone,
    classification.clone,
    location.clone,
    event.clone,
    attribution.clone,
    identification.clone,
    measurement.clone,
    assertions.clone)

  /**
    * Equals implementation that compares the contents of all the contained POSOs
    */
  override def equals(that: Any) = that match {
    case other: FullRecord => {
      if (this.uuid != other.uuid) false
      else if (!EqualsBuilder.reflectionEquals(this.occurrence, other.occurrence)) false
      else if (!EqualsBuilder.reflectionEquals(this.classification, other.classification)) false
      else if (!EqualsBuilder.reflectionEquals(this.location, other.location)) false
      else if (!EqualsBuilder.reflectionEquals(this.event, other.event)) false
      else if (!EqualsBuilder.reflectionEquals(this.attribution, other.attribution, Array("taxonomicHints", "parsedHints"))) {
        false
      }
      else if (!EqualsBuilder.reflectionEquals(this.measurement, other.measurement)) false
      else if (!EqualsBuilder.reflectionEquals(this.identification, other.identification)) false
      else true
    }
    case _ => false
  }

  @JsonIgnore
  def getRawFields(): scala.collection.Map[String, String] = rawFields

  def setRawFieldsWithMapping(rf: scala.collection.Map[String, String]) = {
    rawFields = rf map { case (k, v) => (k.toLowerCase match {
      case "class" | "clazz" | "classs" => "classs"
      case _ => k
    }, v)
    }
  }
}
