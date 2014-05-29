package au.org.ala.biocache.cmd

import au.org.ala.biocache.Config
import java.io.File

/**
 * Trait used by tools providing some incremental update supports.
 * Relevant for processing, sampling and indexing tasks.
 */
trait IncrementalTool {

  /**
   * Retrieves a delete row file that may have been generated as part of an
   * data load/update.
   *
   * @param resourceUid
   * @return
   */
  def getDeleteRowFile(resourceUid:String) : Option[String] = {
    def filename = Config.deletedFileStore + File.separator + resourceUid + File.separator + "deleted.txt"
    if(new File(filename).exists()){
      Some(filename)
    } else {
      None
    }
  }

  /**
   * Checks to see if a row key file is available.
   *
   * @param resourceUid
   * @return
   */
  def hasRowKey(resourceUid: String): (Boolean, Option[String]) = {
    def filename = "/data/tmp/row_key_" + resourceUid + ".csv"
    def file = new java.io.File(filename)

    if (file.exists()) {
      val date = new java.util.GregorianCalendar()
      date.setTime(new java.util.Date)
      date.add(java.util.Calendar.HOUR, -24)
      //if it is on the same day assume that we want the incremental process or index.
      if (org.apache.commons.io.FileUtils.isFileNewer(file, date.getTime())){
        (true, Some(filename))
      } else {
        //prompt the user
        println("There is an incremental row key file for this resource.  Would you like to perform an incremental process (y/n) ?")
        val answer = readLine
        if (answer.toLowerCase().equals("y") || answer.toLowerCase().equals("yes")) {
          (true, Some(filename))
        } else {
          (false, None)
        }
      }
    } else {
      (false, None)
    }
  }
}