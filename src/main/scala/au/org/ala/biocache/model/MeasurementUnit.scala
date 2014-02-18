package au.org.ala.biocache.model

/**
 * Created by mar759 on 17/02/2014.
 */
abstract sealed class MeasurementUnit

case object Metres extends MeasurementUnit
case object Kilometres extends MeasurementUnit
case object Feet extends MeasurementUnit
