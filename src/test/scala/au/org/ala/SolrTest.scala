package au.org.ala

import au.com.bytecode.opencsv.CSVReader
import org.apache.solr.client.solrj.impl.CloudSolrClient

/**
  * Created by mar759 on 19/08/2016.
  */
object SolrTest {

  def main(args:Array[String]) : Unit ={

    val cloudServer = new CloudSolrClient("http://localhost:8983")

//    val file = new CSVReader("/data/")





  }

}
