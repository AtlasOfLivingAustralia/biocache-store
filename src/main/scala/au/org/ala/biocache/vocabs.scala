package au.org.ala.biocache

case class Term (canonical:String, variants:Array[String])

/**
 * Quick state string matching.
 * @author Dave Martin (David.Martin@csiro.au)
 */
object States {
	val act = new Term("Australian Capital Territory", Array("AustralianCapitalTerritory","AusCap","ACT"))
	val nsw = new Term("New South Wales", Array("NEWSOUTHWALES","NSWALES","NSW"))
	val nt = new Term("Northern Territory", Array("NTERRITORY","NTERRIT","NT"))
	val qld = new Term("Queensland", Array("QUEENSLAND","QLD"))
	val sa = new Term("South Australia", Array("SOUTHAUSTRALIA","SAUSTRALIA","SAUST","SA"))
	val tas = new Term("Tasmania", Array("TASMANIA", "TASSIE","TAS"))
	val vic = new Term("Victoria", Array("SAUSTRALIA","SOUTHAUSTRALIA", "SAUST","SA"))
	val wa = new Term("Western Australia", Array("WAUSTRALIA","WESTAUSTRALIA","WA"))
	val all = Array(act,nsw,nt,qld,sa,tas,vic,wa)
	
	def matchTerm(stateString:String) : Option[Term] = {
		if(stateString!=null){
			//strip whitespace & strip quotes and fullstops & uppercase
			val stringToUse = stateString.trim.replace(".","").replace(" ", "").toUpperCase
			for(state<-all){
				if(state.variants.contains(stringToUse)){
					return Some(state)
				}
			}
		}
		None
	}
}

object AssertionCodes {

	val GEOSPATIAL_PRESUMED_NEGATED_LATITUDE = 1
	val GEOSPATIAL_PRESUMED_NEGATED_LONGITUDE = 2
	val GEOSPATIAL_PRESUMED_INVERTED_COORDINATES = 3
	val GEOSPATIAL_ZERO_COORDINATES = 4
	val GEOSPATIAL_COORDINATES_OUT_OF_RANGE = 5

	val GEOSPATIAL_UNKNOWN_COUNTRY_NAME = 7
	val GEOSPATIAL_ALTITUDE_OUT_OF_RANGE = 8
	val GEOSPATIAL_PRESUMED_ERRONOUS_ALTITUDE = 9
	val GEOSPATIAL_PRESUMED_MIN_MAX_ALTITUDE_REVERSED = 10
	val GEOSPATIAL_PRESUMED_DEPTH_IN_FEET = 11
	val GEOSPATIAL_DEPTH_OUT_OF_RANGE = 12
	val GEOSPATIAL_PRESUMED_MIN_MAX_DEPTH_REVERSED = 13
	val GEOSPATIAL_PRESUMED_ALTITUDE_IN_FEET = 14
	val GEOSPATIAL_PRESUMED_ALTITUDE_NON_NUMERIC = 15
	val GEOSPATIAL_PRESUMED_DEPTH_NON_NUMERIC = 16

	
	val GEOSPATIAL_COUNTRY_COORDINATE_MISMATCH = 6
	val GEOSPATIAL_STATE_COORDINATE_MISMATCH = 17
	
	
	val TAXONOMIC_INVALID_SCIENTIFIC_NAME = 1001
	val TAXONOMIC_UNKNOWN_KINGDOM = 1002
	val TAXONOMIC_AMBIGUOUS_NAME = 1003
	val TAXONOMIC_NAME_NOTRECOGNISED = 1004

	val OTHER_MISSING_BASIS_OF_RECORD = 2001
	val OTHER_BADLY_FORMED_BASIS_OF_RECORD = 2002
	val OTHER_INVALID_DATE = 2003
	val OTHER_COUNTRY_INFERRED_FROM_COORDINATES = 2004
}