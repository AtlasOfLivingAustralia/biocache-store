package au.org.ala.biocache.tool

import java.io.{FileReader, File}
import au.com.bytecode.opencsv.CSVReader
import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.util.OptionParser

/**
 * A utility to help migration to a remote media repository.
 */
object MigrateMedia extends Tool {

  import scala.collection.JavaConversions._

  def cmd = "migrate-media"
  def desc = "migrate media from local repository to remote."

  def main(args:Array[String]){

    var dryRun = true
    var sourceFile = ""

    val parser = new OptionParser("migrate-media") {
      arg("migrate-file", "UUID and resource UID file for migrating media.", {
        v: String => sourceFile = v
      })
      booleanOpt("dryRun", "dryRun", "dryRun or not", { v:Boolean => dryRun = v } )
    }
    if(parser.parse(args)) {

      println("Dry run : " + dryRun)

      val reader = new CSVReader(new FileReader(new File(sourceFile)))
      reader.readAll().foreach(line => {
        if(line.length == 2) {
          val uuid = line(0)
          val dataResourceUid = line(1)

          Config.occurrenceDAO.getByRowKey(uuid) match {
            case Some(record) => {
              val filesToSave = record.occurrence.associatedMedia.split(";")
              filesToSave.foreach(filePath => {
                println(s"saving $uuid, $dataResourceUid, $filePath")
                val (alreadyStored, fileName, identifer) = Config.mediaStore.alreadyStored(uuid, dataResourceUid, new File(filePath))
                if (!dryRun && !alreadyStored) {
                  try {
                    Config.mediaStore.save(uuid, dataResourceUid, "file://" + filePath, None)
                  } catch {
                    case e: Exception => println(s"Problem saving $uuid, $dataResourceUid, $filePath")
                  }
                } else {
                  println("Already stored. ID: " + identifer)
                }
              })
            }
          }
        }
      })
    }
  }
}
