/*
 * Copyright (C) 2014 Atlas of Living Australia
 * All Rights Reserved.
 *
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 */
package au.org.ala.biocache.load

import java.net.{HttpURLConnection, URL}
import au.org.ala.biocache.{Store, Config}
import scalaj.http.{HttpOptions, Http}
import au.org.ala.biocache.util.Json
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.collection.mutable.ArrayBuffer

/**
 *
 * Loads the BIE records into the biocache based on a list of exported data resources:
 *
 * 1) Update the collectory
	- set type = "Records"
	- set connection params to: CSV, comma separated, quoted

   2) Ingest into the biocache
 *
 *
 * The BIE resources listed below have some issues related to them. Most of them are not include in the default list
 * of data resource to load from the BIE. Only dr446/dr585 is being included.
 *
 * dr360.csv - Flckr images from BIE only 1686 records these arwe not going to be loaded at the moment.
 *
 * dr441.csv - 11188 Morphbank images that have been harvested into the BIE with different keywords. These will be
 * loaded externally using a local CSV load
 *
 * dr466 - this data resource has the incorrect uid the BIE database - rename the file and directory to dr585
 *
 * dr445 - Mosquitoes of Australia data set change uid to dr533 which is no longer contributing to the ALA so we will ignore it for now.
 *
 * dr440 - These are the images that were uploaded via the ALA web, they don't have external URLs and should potentially be added via CS field data
 * I have put this data resource back in but it can always be deleted if required
 *
 * @author Natasha Quimby
 */
object BieCsvLoader {
  val bieDrString = "dr396,dr414,dr429,dr449,dr462,dr572,dr382,dr399,dr415,dr430,dr450,dr463,dr613,dr384,dr401,dr416,dr431,dr451,dr585,dr627,dr385,dr402,dr417,dr432,dr452,dr469,dr660,dr386,dr403,dr418,dr453,dr521,dr665,dr387,dr405,dr419,dr455,dr524,dr674,dr388,dr406,dr421,dr443,dr457,dr526,dr389,dr408,dr422,dr458,dr532,dr390,dr410,dr427,dr447,dr459,dr534,dr394,dr413,dr428,dr448,dr461,dr545,dr440"
  //val drs = bieDrString.split(",")

  def main(args: Array[String]) {
    val drs = {
      if(args.length==0){
        bieDrString.split(",")
      } else{
        args(0).split(",")
      }
    }
    println("Attempting to load bie resources " + drs.toList)
    val l = new Loader()
    val buf = new ArrayBuffer[String]()
    println(l.resourceList)
    val biocacheResources: List[String] = l.resourceList.map(resource => resource.getOrElse("uid", ""))
    val dataResources = Config.indexDAO.getDistinctValues("*:*", "data_resource_uid",1000).getOrElse(List()).toSet[String]
    println("Data Resources in index : " + dataResources)
    //l.describeResource(drs.toList)
    drs.foreach(dr => {
      println(s"Starting to handle bie data resource $dr")
      //1) Check to see if it is already included in the data resources:
      if (biocacheResources.contains(dr)) {
        println(s"Warning the collectory already contains a Record resource for $dr")
      }
      if(dataResources.contains(dr)){
        println(s"Warning the biocache already contains a Record resource for $dr")
        buf += dr
     } else {
        val fileUrl = s"http://www2.ala.org.au/datasets/occurrence/bie/$dr/$dr.csv"
        //2) Check to see if the file exists
        val connection = new URL(fileUrl).openConnection().asInstanceOf[HttpURLConnection]
        connection.setRequestMethod("HEAD")
        if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
          println("We have a source CSV file we can continue")
          //now we need to update the connection details for the dr
          updateCollectoryForLoad(dr, fileUrl)
          //now ingest this one
          Store.ingest(Array(dr))
        } else {
          println(s"There is no source file for $dr")
          buf +=dr
        }
      }

    })
    println("Warning unable to load " + buf.toList + " resources")
    Config.persistenceManager.shutdown
    Config.indexDAO.shutdown

  }



  def updateCollectoryForLoad(resourceUid:String, fileUrl:String):Boolean={
    val mapper = new ObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.setSerializationInclusion(Include.NON_NULL)
    try {

        val map =new  scala.collection.mutable.HashMap[String,Object]()
        //create the connection params map to add to the overall update map
        val connectParams = Map("protocol"->"DwC","csv_text_enclosure"->"\"", "termsForUniqueKey"->Array("occurrenceID"),
          "csv_eol"->"\n", "csv_delimiter"->",", "csv_escape_char"->"\\", "url"->fileUrl)
        //create the default darwin core values map to add to the overall update map
        val defaultDwcValues = Map("basisOfRecord"->"Image")
        map ++= Map("user"-> Config.getProperty("registry.username"), "api_key"-> Config.getProperty("registry.apikey"),
          "resourceType"->"records" , "connectionParameters"->connectParams, "defaultDarwinCoreValues"->defaultDwcValues)
        val data = mapper.writeValueAsString(map)//Json.toJSONMap(map.toMap)
        //turn the map of values into JSON representation
        println(data)
        val response = Http.postData(Config.registryUrl + "/dataResource/" +resourceUid,data).header("content-type", "application/json")
        response.option(HttpOptions.connTimeout(5000)).option(HttpOptions.readTimeout(5000))
        println("Data resource " + resourceUid + " " + response.responseCode)
      true
    } catch {
      case e:Exception => e.printStackTrace();false
    }
  }

}
