package au.org.ala.util

import au.org.ala.biocache.TaxonProfileDAO
import au.org.ala.biocache.TaxonProfile
import java.net.InetSocketAddress
import org.apache.avro.ipc.SocketTransceiver
import org.apache.avro.specific.SpecificRequestor
import org.apache.avro.util.Utf8
import au.org.ala.bie.rpc.{ ProfileArray, Page, SpeciesProfile }

/**
 * Created by IntelliJ IDEA.
 * User: davejmartin2
 * Date: 12/01/2011
 * Time: 20:40
 * To change this template use File | Settings | File Templates.
 */
object SpeciesProfileLoader {

  def main(args: Array[String]): Unit = {

    println("test")
    import scala.collection.JavaConversions._

    val host = args(0)
    val port = args(1).toInt
    val taxonProfileDAO = new TaxonProfileDAO
    //int maxNoOfProfiles = Integer.MAX_VALUE;
    val client = new SocketTransceiver(new InetSocketAddress(host, port))
    val proxy = SpecificRequestor.getClient(classOf[SpeciesProfile], client).asInstanceOf[SpeciesProfile]
    var page = new Page
    var lastKey = new Utf8("")
    page.startKey = lastKey
    page.pageSize = 1000
    var array = proxy.send(page).asInstanceOf[ProfileArray]
    
    while(array.profiles.size>0){
	    for (profile <- array.profiles) {
	      //add to cache
	      println(profile)
	      var taxonProfile = new TaxonProfile
	      taxonProfile.guid = profile.guid.toString
	      taxonProfile.scientificName  = profile.scientificName.toString
	      if(profile.commonName!=null){
	    	  taxonProfile.commonName = profile.commonName.toString
	      }
	      if(profile.habitat!=null){
	    	  val habitats = for(habitat<-profile.habitat) yield habitat.toString
	    	  taxonProfile.habitat = habitats.toArray
	      }
	      taxonProfileDAO.add(taxonProfile)
	      lastKey = profile.guid
	    }
	    page.startKey = lastKey
	    array = proxy.send(page).asInstanceOf[ProfileArray]
    }
    client.close
  }
}