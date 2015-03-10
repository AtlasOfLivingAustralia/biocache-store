/**
* This script cleanup any derefenced indexes.
*/
import groovy.json.JsonSlurper
statusUrl = new URL("http://localhost:8080/solr/admin/cores?action=STATUS&wt=json")
slurper = new JsonSlurper()
json = slurper.parseText(statusUrl.text)
referencedDirs = []

if(json.status){
  json.status.each {
     coreName = it.key
     dataDir = it.value.get("dataDir")
     referencedDirs << new File(dataDir)
  }
}

referencedDirs = referencedDirs.toSet()

//existing on filesystem referenced
existingDirectories = []

//build up list of non-referenced dir
dereferenced = []

new File("/data/solr-indexes").eachFile { existingDirectories << it }

deferenced = existingDirectories.minus(referencedDirs)

referencedDirs.each {
  println(new Date().toString() + ": currently referenced: " + it.getAbsolutePath())	
}

deferenced.each {
  println(new Date().toString() + ": index available for deletion: " + it.getAbsolutePath())	
  it.deleteDir()
}
