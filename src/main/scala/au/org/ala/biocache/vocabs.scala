package au.org.ala.biocache

import scala.collection.mutable.ArrayBuffer
case class Term (canonical:String, variants:Array[String])

trait Vocab {
  val all:Array[Term]
  /**
   * Match a term. Matches canonical form or variants in array
   * @param string2Match
   * @return
   */
  def matchTerm(string2Match:String) : Option[Term] = {
    if(string2Match!=null){
      //strip whitespace & strip quotes and fullstops & uppercase
      val stringToUse = string2Match.replaceAll("([.,-]*)?([\\s]*)?", "").toLowerCase
      for(term<-all){
        if(term.canonical.equalsIgnoreCase(stringToUse))
          return Some(term)
        if(term.variants.contains(stringToUse)){
          return Some(term)
        }
      }
    }
    None
  }
  /**
   * Retrieve all the terms defined in this vocab.
   * @return
   */
  def retrieveAll : Array[Term] = {
    val methods = this.getClass.getMethods
    for{
      method<-methods
      if(method.getReturnType.getName == "au.org.ala.biocache.Term")
    } yield (method.invoke(this).asInstanceOf[Term])
  }
}

/**
 * Quick state string matching.
 * @author Dave Martin (David.Martin@csiro.au)
 */
object States extends Vocab {
  val act = new Term("Australian Capital Territory", Array("AustCapitalTerritory","AustCapitalTerrit","AusCap","ACT"))
  val nsw = new Term("New South Wales", Array("nswales","nsw"))
  val nt = new Term("Northern Territory", Array("nterritory","nterrit","nt"))
  val qld = new Term("Queensland", Array("qland","qld"))
  val sa = new Term("South Australia", Array("sthaustralia","saustralia","saust","sa"))
  val tas = new Term("Tasmania", Array("tassie","tas"))
  val vic = new Term("Victoria", Array("vic","vict"))
  val wa = new Term("Western Australia", Array("waustralia","westaustralia","westaust","wa"))
  val all = retrieveAll
}

object BasisOfRecord extends Vocab {
  val specimen = new Term("PreservedSpecimen", Array("specimen","s"))
  val observation = new Term("HumanObservation", Array("observation","o"))
  val fossil = new Term("FossilSpecimen", Array("fossil","f"))
  val living = new Term("LivingSpecimen", Array("living","l"))
  val all = retrieveAll
}

object TypeStatus extends Vocab {
  val allolectotype = new Term("allolectotype", Array[String]())
  val alloneotype = new Term("alloneotype", Array[String]())
  val allotype = new Term("allotype", Array[String]())
  val cotype = new Term("cotype", Array[String]())
  val epitype = new Term("epitype", Array[String]())
  val exepitype = new Term("exepitype", Array[String]())
  val exholotype = new Term("exholotype", Array[String]())
  val exisotype = new Term("exisotype", Array[String]())
  val exlectotype = new Term("exlectotype", Array[String]())
  val exneotype = new Term("exneotype", Array[String]())
  val exparatype = new Term("exparatype", Array[String]())
  val exsyntype = new Term("exsyntype", Array[String]())
  val extype = new Term("extype", Array[String]())
  val hapantotype = new Term("hapantotype", Array[String]())
  val holotype = new Term("holotype", Array[String]())
  val iconotype = new Term("iconotype", Array[String]())
  val isolectotype = new Term("isolectotype", Array[String]())
  val isoneotype = new Term("isoneotype", Array[String]())
  val isosyntype = new Term("isosyntype", Array[String]())
  val isotype = new Term("isotype", Array[String]())
  val lectotype = new Term("lectotype", Array[String]())
  val neotype = new Term("neotype", Array[String]())
  val notatype = new Term("notatype", Array[String]())
  val paralectotype = new Term("paralectotype", Array[String]())
  val paraneotype = new Term("paraneotype", Array[String]())
  val paratype = new Term("paratype", Array[String]())
  val plastoholotype = new Term("plastoholotype", Array[String]())
  val plastoisotype = new Term("plastoisotype", Array[String]())
  val plastolectotype = new Term("plastolectotype", Array[String]())
  val plastoneotype = new Term("plastoneotype", Array[String]())
  val plastoparatype = new Term("plastoparatype", Array[String]())
  val plastosyntype = new Term("plastosyntype", Array[String]())
  val plastotype = new Term("plastotype", Array[String]())
  val secondarytype = new Term("secondarytype", Array[String]())
  val supplementarytype = new Term("supplementarytype", Array[String]())
  val syntype = new Term("syntype", Array[String]())
  val topotype = new Term("topotype", Array[String]())
  val typee = new Term("type", Array[String]())
  val all = retrieveAll
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
  val OTHER_UNRECOGNISED_TYPESTATUS = 2006
}