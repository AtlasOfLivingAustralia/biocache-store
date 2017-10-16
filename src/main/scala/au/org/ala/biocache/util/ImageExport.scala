package au.org.ala.biocache.util

import java.io.{File, FileWriter}

import au.com.bytecode.opencsv.CSVWriter
import au.org.ala.biocache.Config
import au.org.ala.biocache.cmd.Tool
import au.org.ala.biocache.persistence.PersistenceManager

import scala.util.parsing.json.JSON

object ImageExport extends Tool {

  def cmd = "image-export"

  def desc = "Exports images into a CSV"

  def main(args: Array[String]) {

    var entity = "occ"
    var imageField = "images" + Config.persistenceManager.fieldDelimiter + "p"
    var exportFile = Config.tmpWorkDir + "/image-export.csv"
    var additionalFields = Array("uuid", "dataResourceUid", "collectionCode", "institutionCode", "scientificName")
    var resources: Seq[String] = List()

    val parser = new OptionParser(help) {
      opt("e", "entity", "occ", "the column family to page through", {
        v: String => entity = v
      })
      opt("if", "imageField", "associatedMedia", "image field", {
        v: String => imageField = v
      })
      opt("f", "export file path", Config.tmpWorkDir + "/image-export.csv", "File to export to", {
        v: String => exportFile = v
      })
      opt("r", "resources", "dr321,dr123", "comma separated list of resources (uids) with images", {
        v: String => resources = v.split(",").map(x => x.trim)
      })
      opt("af", "additional-fields", "\"dataResourceUid,collectionCode,institutionCode,scientificName\"", "comma separated list of additional fields e.g. ", {
        v: String => additionalFields = v.split(",").map(x => x.trim)
      })
    }

    if (parser.parse(args)) {
      val pm = Config.persistenceManager
      val csvWriter = new CSVWriter(new FileWriter(new File(exportFile)))

      if (resources.isEmpty) {
        extractImages(pm, entity, imageField, csvWriter, additionalFields)
      } else {
        resources.foreach(uid => {
          extractImages(pm: PersistenceManager, entity, imageField, csvWriter, additionalFields, uid + "|", uid + "|~")
        })
      }
      csvWriter.flush
      csvWriter.close
      //      pm.shutdown
    }
  }

  def extractImages(pm: PersistenceManager, entity: String, imageField: String, csvWriter: CSVWriter, additionalFields: Seq[String], start: String = "", end: String = "") {
    var images = 0
    var recordCount = 0
    val requiredFields = Array(imageField) ++ additionalFields

    pm.pageOverSelect(entity, (rowKey, fields) => {
      fields.get(imageField) match {
        case Some(associatedMedia) => {
          val jsonParsed = JSON.parseFull(associatedMedia)
          if (!jsonParsed.isEmpty) {
            val filePathArray = jsonParsed.get.asInstanceOf[List[String]]
            filePathArray.foreach(filePath => {
              val toWrite = Array(Config.mediaStore.convertPathToUrl(filePath)) ++ additionalFields.map(field => {
                fields.getOrElse(field, "")
              }).toArray[String]
              csvWriter.writeNext(toWrite)
              images += 1
            })
          }
        }
        case None => //nothing
      }
      recordCount += 100

      csvWriter.flush
      true
    }, start, end, 100, requiredFields: _*)

    println("Finished exporting - line: " + recordCount + ", images: " + images)
  }
}
