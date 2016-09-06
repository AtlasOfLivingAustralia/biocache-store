package au.org.ala.biocache.util

/**
  * Created by mar759 on 8/08/2016.
  */
case class GridRef(
  val gridLetters:String,
  var easting:Int,  //centroid easting
  var northing:Int,  //centroid northing
  var coordinateUncertainty:Option[Int],
  var minEasting:Int,
  var minNorthing:Int,
  var maxEasting:Int,
  var maxNorthing:Int,
  var datum:String
)

