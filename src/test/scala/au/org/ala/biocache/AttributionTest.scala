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

    test("licence lookup based on known supplied licences") {
        var licences = Array(
            ("https://creativecommons.org/publicdomain/zero/1.0/legalcode", "CC0"),
            ("http://creativecommons.org/licenses/by-nc/4.0/", "CC-BY-NC 4.0 (Int)"),
            ("http://creativecommons.org/licenses/by/4.0/", "CC-BY 4.0 (Int)"),
            ("https://creativecommons.org/licenses/by/4.0/", "CC-BY 4.0 (Int)"),
            ("https://creativecommons.org/licenses/by/3.0/au/", "CC-BY 3.0 (Au)"),
            ("http://creativecommons.org/licenses/by-nc-sa/4.0/","CC-BY-NC-SA 4.0 (Int)"),
            ("http://creativecommons.org/licenses/by-nc-nd/4.0/", "CC-BY-NC-ND 4.0 (Int)"),
            ("Attribution-NonCommercial-ShareAlike License", "CC-BY-NC-SA 4.0 (Int)"),
            ("http://creativecommons.org/licenses/cc0/4.0/", "CC0"),
            ("http://creativecommons.org/licenses/by-sa/4.0/", "CC-BY-SA 4.0 (Int)"),
            ("Attribution License", "CC-BY 4.0 (Int)"),
            ("Creative Commons Attribution Non-Commercial Australia 3.0", "CC-BY-NC 3.0 (Au)"),
            ("CCBY 4.0", "CC-BY 4.0 (Int)"),
            ("Attribution-NonCommercial License", "CC-BY-NC 4.0 (Int)"),
            ("Public Domain Mark", "PDM"),
            ("https://creativecommons.org/licenses/by/4.0/legalcode", "CC-BY 4.0 (Int)"),
            ("Attribution-ShareAlike License", "CC-BY-SA 4.0 (Int)"),
            ("� All rights reserved. Image may not be used for any purpose without permission from the copyright holder", "Custom"),
            ("CC-BY 3.0(Au)", "CC-BY 3.0 (Au)"),
            ("http://creativecommons.org/licenses/by-nd/4.0/", "CC-BY-ND 4.0 (Int)"),
            ("All Rights Reserved", "Custom"),
            ("Attribution-NonCommercial-NoDerivs License", "CC-BY-NC-ND 4.0 (Int)"),
            ("Attribution-NoDerivs License", "CC-BY-ND 4.0 (Int)")
        )
        licences.foreach(l => {
            var raw = new FullRecord
            var processed = new FullRecord
            raw.attribution.dataResourceUid = "dr367"
            raw.attribution.license = l._1
            (new AttributionProcessor).process("test", raw, processed)
            expectResult(l._2, l){processed.attribution.license}
        })
    }
}
