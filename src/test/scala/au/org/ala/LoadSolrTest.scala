package au.org.ala

import java.util
import java.util.UUID

import au.org.ala.biocache.Config
import org.apache.http.impl.client.cache.CachingHttpClientBuilder
import org.apache.solr.client.solrj.impl.{CloudSolrClient, ConcurrentUpdateSolrClient}
import org.apache.solr.common.SolrInputDocument

/**
  * Created by mar759 on 19/08/2016.
  */
object LoadSolrTest {

  def main(args:Array[String]) : Unit ={

    val fields = Array(
      "basis_of_record",
      "class",
      "class_id",
      "common_name_and_lsid",
      "country",
      "data_resource_uid",
      "dataset_name",
      "family",
      "family_id",
      "genus",
      "genus_guid",
      "geospatial_kosher",
      "individual_count",
      "kingdom",
      "kingdom_id",
      "location_id",
      "month",
      "name_match_metric",
      "names_and_lsid",
      "occurrence_id",
      "occurrence_status",
      "order",
      "order_id",
      "phylum",
      "phylum_id",
      "places",
      "rank",
      "raw_basis_of_record",
      "raw_datum",
      "raw_latitude",
      "raw_longitude",
      "raw_occurrence_status",
      "raw_taxon_name",
      "species",
      "species_guid",
      "species_list_uid",
      "state",
      "taxon_name",
      "taxonomic_kosher",
      "year"
    )

    val httpClient = CachingHttpClientBuilder.create()
      .setUserAgent(Config.userAgent)
      .useSystemProperties().build()


    val solrServer = new CloudSolrClient.Builder().withZkHost("localhost:9983").withHttpClient(httpClient).build()
    solrServer.setDefaultCollection("biocache")

    val buff = new util.ArrayList[SolrInputDocument]()

    (0 to 1000000).foreach { id =>
      val doc = new SolrInputDocument()
      doc.addField("id",  UUID.randomUUID().toString)

      fields.foreach { field =>
        doc.addField(field, UUID.randomUUID().toString)
      }
      buff.add(doc)

      if(id % 1000 == 0){
        println(id)
          solrServer.add(buff)
          buff.clear()
          solrServer.commit(false, false, true)
      }
    }

    println("finished")

    solrServer.commit()
    solrServer.close()
//    solrServer.shutdownNow()
  }

}
