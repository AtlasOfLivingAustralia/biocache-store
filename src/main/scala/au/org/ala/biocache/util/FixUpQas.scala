package au.org.ala.biocache.util

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import scala.collection.{mutable, JavaConversions}
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.HttpPost
import org.apache.commons.io.IOUtils
import au.org.ala.biocache.Config

object FixUpQas {

  def main(args:Array[String]){

    import JavaConversions._

    val lookupUrl = "http://auth.ala.org.au/userdetails/userDetails/getUserListFull"
    val om = new ObjectMapper()
    val httpClient = new DefaultHttpClient()
    try {
      val httpPost = new HttpPost(lookupUrl)
      val response = httpClient.execute(httpPost)
      try {
        val postBody = IOUtils.toString(response.getEntity().getContent)
        val jsonNode:JsonNode = om.readTree(postBody)
        val userMap = new mutable.HashMap[String,String]

        jsonNode.elements().foreach( node => {
          val email = node.get("email").textValue().toLowerCase()
          val id = node.get("id").intValue().toString()
          println(email + ":" + id)
          userMap.put(email, id)
        })

        //paging through queryassert
        Config.persistenceManager.pageOverAll("queryassert", (key,map) => {
          println(key)
          val userEmail =  map.getOrElse("userName", "")
          Config.persistenceManager.put(key, "queryassert", "userEmail", userEmail, true, false)

          //lookup the CAS ID for this user..
          val id = userMap.get(userEmail)
          if(!id.isEmpty){
            Config.persistenceManager.put(key, "queryassert", "userId", id.get, true, false)
          }
          true
        })

        //paging through qa
        Config.persistenceManager.pageOverAll("qa", (key,map) => {

          println(key)

          val userEmail =  map.getOrElse("userId", "")
          Config.persistenceManager.put(key, "qa", "userEmail", userEmail, true, false)

          //lookup the CAS ID for this user..
          val id = userMap.get(userEmail)
          if(!id.isEmpty){
            Config.persistenceManager.put(key, "qa", "userId", id.get, true, false)
          }

          true
        })
      } finally {
        response.close()
      }
    } finally {
      try {
        httpClient.close()
      } finally {
        Config.persistenceManager.shutdown
      }
    }
  }
}
