package au.org.ala.biocache.load

import au.org.ala.biocache.util.{Json, OptionParser}
import au.org.ala.biocache.Config
import au.org.ala.biocache.model.FullRecord
import scala.collection.mutable.ArrayBuffer
import au.org.ala.biocache.cmd.Tool

/**
 * Utility for downloading the media associated with a resource and caching
 * locally.
 */
object DownloadMedia extends Tool {

  def cmd = "download-media"
  def desc = "Download the associated media for a resource"

  def main(args:Array[String]){
    var dr: String = ""
    var rowKey: String = ""
    val parser = new OptionParser(help) {
      opt("dr","data-resource-uid", "The resource to page over and download the media for", { v: String => dr = v })
      opt("rowkey","row-key-record", "The rowkey for record", { v: String => rowKey = v })
    }

    if (parser.parse(args)) {
      if (dr != "") processDataResource(dr)
      else if (rowKey != "") processRecord(rowKey)
      else parser.showUsage
    }
//    Config.persistenceManager.shutdown
  }

  /**
   * Split the associated media string into multiple URLs or paths.
   * This string can be delimited by semi-colon or comma.
   *
   * @param associatedMedia
   */
  def unpackAssociatedMedia(associatedMedia:String) : Seq[String] = {
    if(associatedMedia == null || associatedMedia.trim() == 0){
      Array[String]()
    } else if(associatedMedia.indexOf('|') > 0){
      splitByChar(associatedMedia, '|') //pipe is the default in DwC
    } else if(associatedMedia.indexOf(';') > 0){
      splitByChar(associatedMedia, ';')
    } else if(associatedMedia.indexOf(',') > 0){
      splitByChar(associatedMedia, ',')
    } else {
      //no delimiters in use
      Array(associatedMedia)
    }
  }

  def splitByChar(associatedMedia: String, char:Char): Array[String] = {
    val parts = associatedMedia.split(char).map(_.trim)
    def mediaUrl(url:String) =  url.startsWith("http") || url.startsWith("ftp") || url.startsWith("file:")
    if (parts.forall(mediaUrl(_)) || parts.forall(!mediaUrl(_))) {
      parts
    } else {
      Array(associatedMedia)
    }
  }

  def processUrls(raw:FullRecord, processed:FullRecord, urls: Array[String]) {
    val imageUrls = urls.filter(Config.mediaStore.isValidImageURL(_))
    val mediaStorePaths = new ArrayBuffer[String]
    imageUrls.foreach(imageUrl => {
      //download it, store it update processed
      try {
        Config.mediaStore.save(raw.uuid, raw.attribution.dataResourceUid, imageUrl, None) match {
          case Some((filename, filepath)) => mediaStorePaths += filepath
        }
      } catch {
        case e: Exception => println("Problem downloading from URL: " + imageUrl)
      }
    })
    //update the processed.occurrence.images
    Config.persistenceManager.put(raw.rowKey, "occ", "associatedMedia", mediaStorePaths.toArray.mkString(";"))
    Config.persistenceManager.put(raw.rowKey, "occ", "images.p", Json.toJSON(mediaStorePaths.toArray))
  }

  /**
   * Process this single record.
   */
  def processRecord(rowKey:String){
    Config.occurrenceDAO.getRawProcessedByRowKey(rowKey) match {
        case Some(rp) => {
           val  (raw, processed) = (rp(0), rp(1))
           if(raw.occurrence.associatedMedia != null && raw.occurrence.associatedMedia !=""){
             val urls = raw.occurrence.associatedMedia.split(";").map(url => url.trim)
             processUrls(raw, processed, urls)
          }
        }
        case None => println("Unrecognised rowkey..." + rowKey)
      }
    }

  /**
   * Download the media for this resource
   */
  def processDataResource(dr:String){
      Config.occurrenceDAO.pageOverRawProcessed(recordWithOption => {
        if (!recordWithOption.isEmpty){
          val (raw, processed) = recordWithOption.get
          if(raw.occurrence.associatedMedia != null && raw.occurrence.associatedMedia !=""){
            val urls = raw.occurrence.associatedMedia.split(";").map(url => url.trim)
            processUrls(raw, processed, urls)
          }
        }
        true
      }, dr + "|", dr + "|~")
    }
}
