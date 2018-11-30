package au.org.ala.biocache

import au.org.ala.biocache.model.{FullRecord}
import au.org.ala.biocache.processor.LocationProcessor
import au.org.ala.biocache.vocab.GeodeticDatum
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DatumTest extends ConfigFunSuite {

  test("Run tests against raw datum extract"){

    val input = this.getClass.getResourceAsStream("/testDatums.txt")
    val testSet = scala.io.Source.fromInputStream(input).getLines()
    var matches = 0
    var noMatch = 0
    testSet.foreach { test =>
      GeodeticDatum.matchTerm(test) match {
        case Some(term) =>  matches +=1;
        case None =>  noMatch+=1;
      }
    }
    expectResult(true){ matches >= 5691 }
  }

  test("AGD66 re-projection"){

    val raw = new FullRecord
    val processed = new FullRecord
    processed.classification.setScientificName("Cataxia maculata")
    processed.classification.setTaxonConceptID("urn:lsid:biodiversity.org.au:afd.taxon:41cc4a69-06d4-4591-9afe-7af431b7153c")
    processed.classification.setTaxonRankID("7000")
    raw.location.decimalLatitude = "-27.5623432"
    raw.location.decimalLongitude = "152.28342342"
    raw.location.geodeticDatum = "AGD66"
    raw.attribution.dataResourceUid = "dr359"

    val l = new LocationProcessor
    val assertions = l.process("test", raw, processed, None)

    expectResult("EPSG:4326") { processed.location.geodeticDatum }
    expectResult(true) { processed.location.decimalLatitude != raw.location.decimalLatitude}
    expectResult(true) { processed.location.decimalLongitude != raw.location.decimalLongitude}
  }

  test("AGD84/66 re-projection"){

    val raw = new FullRecord
    val processed = new FullRecord
    processed.classification.setScientificName("Cataxia maculata")
    processed.classification.setTaxonConceptID("urn:lsid:biodiversity.org.au:afd.taxon:41cc4a69-06d4-4591-9afe-7af431b7153c")
    processed.classification.setTaxonRankID("7000")
    raw.location.decimalLatitude = "-27.5623432"
    raw.location.decimalLongitude = "152.28342342"
    raw.location.geodeticDatum = "AGD84/66"
    raw.attribution.dataResourceUid = "dr359"

    val l = new LocationProcessor
    val assertions = l.process("test", raw, processed, None)

    expectResult("EPSG:4326") { processed.location.geodeticDatum }
    expectResult(true) { processed.location.decimalLatitude != raw.location.decimalLatitude}
    expectResult(true) { processed.location.decimalLongitude != raw.location.decimalLongitude}
  }

  test("NAD83 - recognised"){

    val raw = new FullRecord
    val processed = new FullRecord
    processed.classification.setScientificName("Cataxia maculata")
    processed.classification.setTaxonConceptID("urn:lsid:biodiversity.org.au:afd.taxon:41cc4a69-06d4-4591-9afe-7af431b7153c")
    processed.classification.setTaxonRankID("7000")
    raw.location.decimalLatitude = "-27.5623432"
    raw.location.decimalLongitude = "152.28342342"
    raw.location.geodeticDatum = "NAD83"
    raw.attribution.dataResourceUid = "dr359"

    val l = new LocationProcessor
    val assertions = l.process("test", raw, processed, None)

    expectResult("EPSG:4326") { processed.location.geodeticDatum }
    expectResult(true) { processed.location.decimalLatitude != raw.location.decimalLatitude}
    expectResult(true) { processed.location.decimalLongitude != raw.location.decimalLongitude}
  }

  test("NZGD49 - recognised"){

    val raw = new FullRecord
    val processed = new FullRecord
    processed.classification.setScientificName("Cataxia maculata")
    processed.classification.setTaxonConceptID("urn:lsid:biodiversity.org.au:afd.taxon:41cc4a69-06d4-4591-9afe-7af431b7153c")
    processed.classification.setTaxonRankID("7000")
    raw.location.decimalLatitude = "-43.5321"
    raw.location.decimalLongitude = "172.6362"
    raw.location.geodeticDatum = "NZGD49"
    raw.attribution.dataResourceUid = "dr359"

    val l = new LocationProcessor
    val assertions = l.process("test", raw, processed, None)

    expectResult("EPSG:4326") { processed.location.geodeticDatum }
    expectResult(true) { processed.location.decimalLatitude != raw.location.decimalLatitude}
    expectResult(true) { processed.location.decimalLongitude != raw.location.decimalLongitude}
  }

  test("NZGD1949 - recognised"){

    val raw = new FullRecord
    val processed = new FullRecord
    processed.classification.setScientificName("Cataxia maculata")
    processed.classification.setTaxonConceptID("urn:lsid:biodiversity.org.au:afd.taxon:41cc4a69-06d4-4591-9afe-7af431b7153c")
    processed.classification.setTaxonRankID("7000")
    raw.location.decimalLatitude = "-43.5321"
    raw.location.decimalLongitude = "172.6362"
    raw.location.geodeticDatum = "NZGD1949"
    raw.attribution.dataResourceUid = "dr359"

    val l = new LocationProcessor
    val assertions = l.process("test", raw, processed, None)

    expectResult("EPSG:4326") { processed.location.geodeticDatum }
    expectResult(true) { processed.location.decimalLatitude != raw.location.decimalLatitude}
    expectResult(true) { processed.location.decimalLongitude != raw.location.decimalLongitude}
  }

  test("Junk test - no projection"){

    val raw = new FullRecord
    val processed = new FullRecord
    processed.classification.setScientificName("Cataxia maculata")
    processed.classification.setTaxonConceptID("urn:lsid:biodiversity.org.au:afd.taxon:41cc4a69-06d4-4591-9afe-7af431b7153c")
    processed.classification.setTaxonRankID("7000")
    raw.location.decimalLatitude = "-27.5623432"
    raw.location.decimalLongitude = "152.28342342"
    raw.location.geodeticDatum = "sjdsakjdkjskaldj"
    raw.attribution.dataResourceUid = "dr359"

    val l = new LocationProcessor
    val assertions = l.process("test", raw, processed, None)

    expectResult(null) { processed.location.geodeticDatum }
    expectResult(true) { processed.location.decimalLatitude == raw.location.decimalLatitude}
    expectResult(true) { processed.location.decimalLongitude == raw.location.decimalLongitude}
  }
}
