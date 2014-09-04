package au.org.ala.biocache.util

import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.Config
import java.util.Date

/**
 * View records with timestamp
 */
object DebugRecord extends Tool {

  def desc = "Retrieve a record details with timestamps"
  def cmd = "debug-record"

  def main(args:Array[String]){
    var rowKey = ""
    var entity = "occ"
    val parser = new OptionParser("copy column options") {
      arg("rowKey", "rowKey.", {
        v: String => rowKey = v
      })
      opt("e", "entity", "The entity (defaults to occ)", {
        v: String => entity = v
      })
    }
    if (parser.parse(args)) {
      var lookup = Config.persistenceManager.getColumnsWithTimestamps(rowKey, entity)
      if(lookup.isEmpty && entity == "occ"){
        val lookupByUUID = Config.persistenceManager.getByIndex(rowKey, "occ", "uuid")
        if(!lookupByUUID.isEmpty && lookupByUUID.get.getOrElse("rowKey", "") != ""){
          val rowKeyFound = lookupByUUID.get.getOrElse("rowKey", "")
          lookup = Config.persistenceManager.getColumnsWithTimestamps(rowKeyFound, "occ")
        }
      }

      if(!lookup.isEmpty){
        val map = lookup.get
        println("\n## Raw values ## ")
        map.keySet.toList.sorted.foreach(key =>
          if(!key.endsWith(".p") && !key.endsWith(".qa")) {
            println(padElementTo25(key) + " updated: " + new Date(map.get(key).get.toLong))
          }
        )

        println("\n## Processed values ## ")
        map.keySet.toList.sorted.foreach(key =>
          if(key.endsWith(".p")) {
            println(padElementTo25(key) + " updated: " + new Date(map.get(key).get.toLong))
          }
        )

        println("\n## Quality assertion values ## ")
        map.keySet.toList.sorted.foreach(key =>
          if(key.endsWith(".qa")) {
            println(padElementTo25(key) + " updated: " + new Date(map.get(key).get.toLong))
          }
        )
      }
    }
  }

  def padAndPrint(str: String) = println(padElementTo25(str))

  def padElementTo25(str: String) = padElement(str, 25)

  def padElement(str: String, width: Int) = str + ( " " * (width - str.length()) )
}
