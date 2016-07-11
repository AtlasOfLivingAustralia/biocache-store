package au.org.ala.biocache.tool

import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.util.{Json, OptionParser}
import au.com.bytecode.opencsv.CSVReader
import java.io.{File, FileReader}
import au.org.ala.biocache.Config
import scala.collection.mutable.ArrayBuffer
import scala.slick.direct.AnnotationMapper.column

/**
 * Load exported references from remote media store.
 *
 *
 */
object LoadMediaReferences extends Tool {

  import scala.collection.JavaConversions._

  def cmd = "load-media-references"

  def desc = "load media references"

  def main(args: Array[String]) {

    var dryRun = false
    var sourceFile = ""

    val parser = new OptionParser(cmd) {
      arg("references-file", "Record UUID, Image UUID, and mime type.", {
        v: String => sourceFile = v
      })
      opt("dryRun", "dryRun", "dryRun or not", {
        dryRun = true
      })
    }

    if (parser.parse(args)) {
      println("Dry run : " + dryRun)
      val reader = new CSVReader(new FileReader(new File(sourceFile)))

      var currentUuid = ""
      var currentMime = ""
      var mediaBuffer = new ArrayBuffer[String]
      var counter = 0

      reader.readAll().foreach(line => {
        counter += 1
        println("Line: " + counter)
        if (line.length == 3) {
          val uuid = line(0)
          val imageUuid = line(1)
          val mimeType = line(2)
//          println(s"Record: $uuid, ImageID: $imageUuid, Mime type: $mimeType")

          if(currentUuid == ""){
            currentUuid = uuid
            currentMime = mimeType
          }

          if (uuid == currentUuid) {
            mediaBuffer += imageUuid
          } else {
            //flush buffer
            val result = Config.occurrenceDAO.getRowKeyFromUuid(currentUuid)
            result match {
              case Some(rowKey) => {
                val column = if (currentMime.startsWith("image")) {
                  "images"
                } else {
                  "sounds"
                }
                if (!dryRun) {
                  Config.persistenceManager.put(rowKey, "occ", column, Json.toJSON(mediaBuffer.toArray), false)
                } else {
                  val buffSize = mediaBuffer.size
                  println(s"DRYRUN : $currentUuid, size: $buffSize rowKey: '$rowKey', current mime: $currentMime, $column : " + Json.toJSON(mediaBuffer.toArray))
                }
              }
            }
            //clear the buffer
            mediaBuffer.clear()
            mediaBuffer += imageUuid
            currentUuid = uuid
            currentMime = mimeType
          }
        }
      })
    }
  }
}
