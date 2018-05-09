package au.org.ala

import org.apache.solr.client.solrj.io.stream.{CloudSolrStream, StreamContext}
import org.apache.solr.common.params.ModifiableSolrParams

/**
  * Created by mar759 on 19/08/2016.
  */
object SolrTest {

  val fields = Array(
    "id",
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

  def main(args:Array[String]) : Unit ={

    val start = System.currentTimeMillis()

    val paramsLoc = new ModifiableSolrParams
    paramsLoc.set("q", "*:*")
    paramsLoc.set("qt", "/export")
    paramsLoc.set("sort", "id asc")
    paramsLoc.set("fl", fields.mkString(","))
    paramsLoc.set("wt", "json")
    paramsLoc.set("rows", "100000000")

    val solrStream = new CloudSolrStream("localhost:9983", "biocache", paramsLoc)
    val context = new StreamContext
    var count = 0
    solrStream.setStreamContext(context)
    try {
      solrStream.open()
      var continue = true
      while ( {
        continue
      }) {
        val tuple = solrStream.read
        if (tuple.EOF){
          System.out.println("End of file")
          continue = false
        } else {
          count += 1
        }
      }
    } finally solrStream.close()

    val timeTakenMillSec = System.currentTimeMillis() - start

    System.out.println("Number of rows: " + count + ", exported in " +  (timeTakenMillSec / 1000) +  ", records per sec: " +  (count / (timeTakenMillSec / 1000)))
  }
}
