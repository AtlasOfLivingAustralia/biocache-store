package au.org.ala.biocache.processor

import au.org.ala.biocache.caches.AttributionDAO
import au.org.ala.biocache.parser.DateParser
import au.org.ala.biocache.model.{QualityAssertion, FullRecord}
import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.model.{FullRecord, QualityAssertion}
import au.org.ala.biocache.parser.DateParser

/**
 * Maps the default values from the data resource configuration in the
 * collectory to the processed record when no raw value exists. This enables basisOfRecord, for example, to be set for
 * all records in a resource when a value is not present in the raw data.
 *
 * This processor should be run before the others so that the default values are populated before reporting
 * missing values
 *
 * This processor also restore the default values.  IMPLICATION is the LocationProcessor needs to be run after
 * to allow sensitive species to be dealt with properly.
 */
class DefaultValuesProcessor extends Processor {

  def process(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {

    //add the default dwc fields if their is no raw value for them.
    val dr = AttributionDAO.getDataResourceByUid(raw.attribution.dataResourceUid)
    if (!dr.isEmpty) {
      if (dr.get.defaultDwcValues != null) {
        dr.get.defaultDwcValues.foreach {
          case (key, value) => {
            if (raw.getProperty(key).isEmpty) {
              //set the processed value to the default value
              processed.setProperty(key, value)
              raw.setProperty(key, value)
              if (!processed.getDefaultValuesUsed && !processed.getProperty(key).isEmpty){
                processed.setDefaultValuesUsed(true)
              }
            }
          }
        }
      }
    }

    //reset the original sensitive values for use in subsequent processing.
    //covers all values that could have been change - thus allowing event dates to be processed correctly...
    //Only update the values if the record has NOT been reloaded since the last processing.
    val lastLoadedDate = DateParser.parseStringToDate(raw.lastModifiedTime)
    val lastProcessedDate = if (lastProcessed.isEmpty) {
      None
    } else {
      DateParser.parseStringToDate(lastProcessed.get.lastModifiedTime)
    }

    if (raw.occurrence.originalSensitiveValues != null && (lastLoadedDate.isEmpty || lastProcessedDate.isEmpty || lastLoadedDate.get.before(lastProcessedDate.get))) {
      FullRecordMapper.mapPropertiesToObject(raw, raw.occurrence.originalSensitiveValues)
    }

    Array()
  }

  def skip(guid: String, raw: FullRecord, processed: FullRecord, lastProcessed: Option[FullRecord] = None): Array[QualityAssertion] = {
    //DefaultValuesProcessor has low overhead, do not skip
    process(guid, raw, processed, lastProcessed)
  }

  def getName = "default"
}
