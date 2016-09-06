package au.org.ala.biocache.tool

import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.util.OptionParser

object CreateSecondaryIndex extends Tool {

  def cmd = "create-index"
  def desc = "Creates a secondary index table"

  def main(args:Array[String]){

    var entityName = ""
    var indexField = ""
    var threads = 4

    val parser = new OptionParser(help) {
      arg("entity-name", "Entity name e.g. occ", {
        v:String => entityName = v.trim
      })
      arg("field-to-index", "Field to index e.g. uuid", {
        v:String => indexField = v.trim
      })
      intOpt("t", "no-of-threads", "The number of threads to use", {v:Int => threads = v } )
    }

    if(parser.parse(args)) {
      // create table
      println("Creating index")
      Config.persistenceManager.createSecondaryIndex(entityName, indexField, threads)
      println("Done")
      Config.persistenceManager.shutdown
    }
  }
}