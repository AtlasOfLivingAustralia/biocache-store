package au.org.ala.biocache
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.junit.Ignore
import au.org.ala.biocache.caches.AttributionDAO
import au.org.ala.biocache.model.FullRecord
import au.org.ala.biocache.processor.AttributionProcessor

@RunWith(classOf[JUnitRunner])
class AttributionTest extends ConfigFunSuite{
       
    test("Test DR lookup in collectory"){
        val dr = AttributionDAO.getDataResourceFromWS("dr367")
        expectResult(true){dr.get.hasMappedCollections}
        expectResult("dp33"){dr.get.dataProviderUid}
    }
    
    test("Collection lookup"){
        var raw = new FullRecord
        var processed = new FullRecord
        raw.attribution.dataResourceUid = "dr367"
        raw.occurrence.collectionCode = "WINC"
        (new AttributionProcessor).process("test", raw, processed)
        expectResult("dp33"){processed.attribution.dataProviderUid}
        expectResult("co74"){processed.attribution.collectionUid}
        
        raw = new FullRecord
        processed = new FullRecord
        raw.attribution.dataResourceUid = "dr360"
        raw.occurrence.collectionCode="TEST"
        val qas = (new AttributionProcessor).process("test", raw, processed)
        expectResult("dp29"){processed.attribution.dataProviderUid}
        expectResult(0){qas.size}
    }
    
    test("Default DWC Values in DR Lookup"){
        val dr = AttributionDAO.getDataResourceFromWS("dr92")
        expectResult(Some("MachineObservation")){dr.get.defaultDwcValues.get("basisOfRecord")}
    }

    test("licence lookup with embedded licence"){
        var raw = new FullRecord
        var processed = new FullRecord
        raw.attribution.dataResourceUid = "dr367"
        raw.attribution.license = "CC-BY Au"
        (new AttributionProcessor).process("test", raw, processed)
        expectResult("CC-BY 4.0 (Au)"){processed.attribution.license}
      }

    test("licence lookup with default licence"){
        var raw = new FullRecord
        var processed = new FullRecord
        raw.attribution.dataResourceUid = "dr366"
        (new AttributionProcessor).process("test", raw, processed)
        expectResult("CC-BY 4.0 (Int)"){processed.attribution.license}
    }
}
