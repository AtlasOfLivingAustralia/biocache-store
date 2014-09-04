/**
 * ************************************************************************
 *  Copyright (C) 2010 Atlas of Living Australia
 *  All Rights Reserved.
 *
 *  The contents of this file are subject to the Mozilla Public
 *  License Version 1.1 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://www.mozilla.org/MPL/
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 * *************************************************************************
 */
package au.org.ala.biocache.util

import java.io._
import java.util.jar.JarFile
import java.util.zip.GZIPInputStream
import au.com.bytecode.opencsv.CSVReader
import net.lingala.zip4j.core.ZipFile
import org.slf4j.LoggerFactory

/**
 * File helper - used as a implicit converter to add additional helper methods to java.io.File
 */
class FileHelper(file: File) {

  val logger = LoggerFactory.getLogger("FileHelper")

  //Appends the supplied file to this one
  def append(afile:FileHelper){
    val writer = new FileWriter(file, true)
    afile.foreachLine(line => writer.write(line+"\n"))
    writer.flush
    writer.close
  }

  def write(text: String){
    val fw = new FileWriter(file)
    try { fw.write(text) }
    finally { fw.close }
  }

  def foreachLine(proc: String => Unit) {
    val br = new BufferedReader(new FileReader(file))
    try {
      while (br.ready) {
        proc(br.readLine)
      }
    } catch {
      case e:Exception => logger.error(e.getMessage, e)
    } finally {
      br.close
    }
  }

  def deleteAll {
    def deleteFile(dfile: File): Unit = {
      if (dfile.isDirectory) {
        val subfiles = dfile.listFiles
        if (subfiles != null)
          subfiles.foreach { f => deleteFile(f) }
      }
      dfile.delete
    }
    deleteFile(file)
  }

  def extractGzip : File = {
    val maxBuffer = 8000
    val basename = file.getName.substring(0, file.getName.lastIndexOf("."))
    val todir = new File(file.getParentFile, basename)
    val in = new GZIPInputStream(new FileInputStream(file), maxBuffer)
    val out = new FileOutputStream(todir)
    try {
      val buffer = new Array[Byte](maxBuffer)
      def read(){
        val byteCount = in.read(buffer)
        if(byteCount >= 0){
          out.write(buffer, 0, byteCount)
          read()
        }
      }
      read()
    } finally {
      in.close
      out.flush
      out.close
    }
    todir
  }

  def extractZip : File = {
    val basename = file.getName.substring(0, file.getName.lastIndexOf("."))
    val todir = new File(file.getParentFile, basename)
    val zipFile = new ZipFile(file)
    zipFile.extractAll(todir.getAbsolutePath)
    todir
  }

  /**
   * Read this file as a CSV
   */
  def readAsCSV(separator:Char, quotechar:Char, procHdr:(Seq[String] => Seq[String]), read:((Seq[String], Seq[String]) => Unit)){
    val reader =  new CSVReader(new FileReader(file), separator, quotechar)
    val rawColumnHdrs = reader.readNext
    val columnHdrs = procHdr(rawColumnHdrs)
    var currentLine = reader.readNext
    while (currentLine != null){
      read(columnHdrs, currentLine)
      currentLine = reader.readNext
    }
  }
}

/**
 * Define a extensions to java.io.File
 */
object FileHelper {
  implicit def file2helper(file: File) = new FileHelper(file)
  def main(args:Array[String]){
    println(org.apache.commons.lang.time.DateUtils.truncate(new java.util.Date(), java.util.Calendar.DAY_OF_MONTH))
  }
}