package au.org.ala.biocache

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import au.org.ala.biocache.model.FullRecord
import au.org.ala.biocache.processor.ClassificationProcessor
import au.org.ala.biocache.vocab.AssertionCodes
import org.gbif.api.vocabulary.NameType

@RunWith(classOf[JUnitRunner])
class TaxonomicNameTest extends ConfigFunSuite {

//    test("taxonomic hint fails"){
//      val raw = new FullRecord
//      val processed = new FullRecord
//      raw.attribution.dataResourceUid="dr376"
//      raw.classification.scientificName="Macropus rufus"
//      val qas = (new ClassificationProcessor).process("test",raw,processed)
//      //we don't want a scientific name
//      expectResult(null){processed.classification.scientificName}
//      expectResult(0){qas.find(_.code == AssertionCodes.RESOURCE_TAXONOMIC_SCOPE_MISMATCH.code).get.qaStatus}
//    }

    test("recursive issue"){
      val raw = new FullRecord
      val processed = new FullRecord
      raw.classification.scientificName = "Pseudosuberia genthi"
      raw.classification.phylum = "Cnidaria"
      raw.classification.kingdom = "Animalia"
      raw.classification.genus = "Pseudosuberia"
      raw.classification.family = "Briareidae"
      val qas = (new ClassificationProcessor).process("test", raw, processed)
//      println(processed.classification.scientificName)
    }
  
    test("name not recognised"){
        val raw = new FullRecord
        val processed = new FullRecord
        raw.classification.scientificName = "dummy name"
        val qas = (new ClassificationProcessor).process("test", raw, processed)
        expectResult(0){qas.find(_.code == 10004).get.qaStatus}
    }

    test("Parse type"){
      val raw = new FullRecord
      val processed = new FullRecord
      raw.classification.scientificName ="Zabidius novemaculeatus"
      (new ClassificationProcessor).process("test",raw,processed)
      expectResult(NameType.SCIENTIFIC.toString){processed.classification.nameParseType}
    }

    ignore("name not in national checklists"){
        val raw = new FullRecord
        val processed = new FullRecord

        //TODO the tests below

//        raw.classification.scientificName = "Amanita farinacea"
//        expectResult(1){
//          val qas = (new ClassificationProcessor).process("test", raw, processed)
//          qas.find(_.code == AssertionCodes.NAME_NOT_IN_NATIONAL_CHECKLISTS.code).get.qaStatus
//        }
//
        //indian mynah - not in AFD currently
        raw.classification.scientificName = "Acridotheres tristis"
        expectResult(0){
          val qas = (new ClassificationProcessor).process("test", raw, processed)
          qas.find(_.code == 10005).get.qaStatus
        }
    }

    ignore("homonym issue"){
        val raw = new FullRecord
        val processed = new FullRecord
        raw.classification.genus = "Thalia"
        raw.classification.scientificName = "Thalia ?"
        val qas = (new ClassificationProcessor).process("test", raw, processed)
//        println(processed.classification.taxonConceptID)
        expectResult(true){processed.classification.getTaxonomicIssue().contains("homonym")}
        expectResult(true){processed.classification.getTaxonomicIssue().contains("questionSpecies")}
//        expectResult(10006){qas(0).code}
    }

    ignore("cross rank homonym resolved"){
      val raw = new FullRecord
      var processed = new FullRecord

      raw.classification.scientificName = "Thalia"
      //raw.classification.family = "Dilleniaceae"
      //unresolved cross rank homonym
      var qas = (new ClassificationProcessor).process("test", raw, processed);
      expectResult(true){processed.classification.getTaxonomicIssue().contains("homonym")}

      //resolve the homonym by setting the rank
      processed = new FullRecord
      raw.classification.taxonRank ="order"
      qas = (new ClassificationProcessor).process("test", raw, processed);
      expectResult(false){processed.classification.getTaxonomicIssue().contains("homonym")}
      expectResult("Termitoidae"){processed.classification.scientificName}
      expectResult("Animalia".toLowerCase()){processed.classification.kingdom.toLowerCase()}
    }


  //    test("missing accepted name"){
//      val raw = new FullRecord
//      var processed = new FullRecord
//      raw.classification.scientificName="Gnaphalium collinum"
//      (new ClassificationProcessor).process("test", raw, processed)
//      expectResult(null){processed.classification.scientificName}
//      //raw.classification.genus = "Gnaphalium"
//      raw.classification.family ="Asteraceae"
//      (new ClassificationProcessor).process("test", raw, processed)
//      expectResult("Asteraceae"){processed.classification.scientificName}
//    }
}