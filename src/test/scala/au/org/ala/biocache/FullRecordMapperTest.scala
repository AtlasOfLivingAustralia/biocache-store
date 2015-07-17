package au.org.ala.biocache

import au.org.ala.biocache.load.FullRecordMapper
import au.org.ala.biocache.model.Versions
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FullRecordMapperTest extends ConfigFunSuite {

  test("Test mapping of 'class'"){

    val fullRecord = FullRecordMapper.createFullRecord(
      "test",
      Map(
        "class" -> "Aves",
        "genus" -> "Platycercus",
        "specificEpithet" -> "elegans"
      ),
      Versions.RAW
    )
    expectResult("Platycercus"){ fullRecord.getClassification.getGenus }
    expectResult("Aves"){ fullRecord.getClassification.getClasss }
  }

  test("Test wrong case"){

    val fullRecord = FullRecordMapper.createFullRecord(
      "test",
      Map(
        "SCIENTIFICNAME" -> "Aves"
      ),
      Versions.RAW
    )
    expectResult("Aves"){ fullRecord.getClassification.getScientificName }

    expectResult(true){ fullRecord.getPropertyNames.contains("scientificName") }
    expectResult(true){ fullRecord.hasProperty("scientificname") }
    expectResult(true){ fullRecord.hasProperty("scientificName") }
  }
}
