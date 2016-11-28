package au.org.ala.biocache.cmd

import au.org.ala.biocache.Config
import au.org.ala.biocache.index.{IndexMergeTool, BulkProcessor, OptimiseIndex, IndexRecords}
import au.org.ala.biocache.load._
import au.org.ala.biocache.tool._
import au.org.ala.biocache.outliers.ReverseJacknifeProcessor
import au.org.ala.biocache.util.{DebugRecord, ImageExport}
import au.org.ala.biocache.export._
import scala.Some
import au.org.ala.biocache.qa.ValidationRuleRunner
import org.drools.core.factmodel.traits.Trait
import scala.collection.JavaConversions

/**
 * A trait to be added to any runnable application within the biocache-store library.
 * Tools are executable standalone or through CMD2.scala
 */
trait Tool {
  def cmd : String
  def desc : String
  def help  = cmd
  def main(args:Array[String]) : Unit
}

trait NoArgsTool extends Tool {

  /**
   * Checks for arguments. If supplied return help.
   * @param args
   */
  def proceed(args:Array[String], proc:() => Unit){
    if(!args.isEmpty){
      println("Usage: " + help)
      println("\tNo arguments required for tool")
    } else {
      proc()
    }
  }
}

object ShowConfig extends NoArgsTool {

  def cmd = "config"
  def desc = "Output the current config for this instance of biocache CLI"
  def main(args:Array[String]) = proceed(args, () => Config.outputConfig)
}

object ShowVersion extends NoArgsTool {

  def cmd = "version"
  def desc = "Show build version information"
  def main(args:Array[String]) = {
    proceed(args, () => run())
  }

  def run() {
    import JavaConversions._
    val propertyNames = Config.versionProperties.stringPropertyNames().toList
    if(propertyNames.nonEmpty) {
      propertyNames.foreach { name =>
        println(name + " = " + Config.versionProperties.getProperty(name))
      }
    } else {
      println("No version information available.")
    }
  }
}

object CMD2 {

  def main(args: Array[String]) {

    import JavaConversions._

    if(args.contains("--version") || args.contains("-version")){
      ShowVersion.main(Array[String]())
      return
    }

    if (args.isEmpty) {

      val versionInfo =  if(!Config.versionProperties.isEmpty) {
        "Commit ID: " + Config.versionProperties.getProperty("gitCommitID") +
          "\n Build date: " +  Config.versionProperties.getProperty("buildTimestamp") +
          "\n For more detail run with --version or type 'version' at the prompt"
      } else {
        "Version information not available"
      }

      println("-" * 80)
      println(" Biocache management tool ")
      println(" " + versionInfo + " ")
      println("-" * 80)
      print("\nPlease supply a command or hit ENTER to view command list. \nbiocache> ")

      var input = readLine
      while (input != "exit" && input != "q" && input != "quit") {
        val myArgs = org.apache.tools.ant.types.Commandline.translateCommandline(input)
        if(!myArgs.isEmpty){
          findTool(myArgs.head) match {
            case Some(tool) => try {
              tool.main(myArgs.tail)
            } catch {
              case e:Exception => {
                e.printStackTrace()
                print("\nbiocache> ")
              }
            }
            case None => {
              printTools
              print("\nbiocache> ")
            }
          }
        } else {
          printTools
        }
        print("\nbiocache> ")
        input = readLine
      }
    } else {

      if(args(0).toLowerCase == "--help"){
        printTools
      } else if(args(0).toLowerCase == "--full-help"){
        tools.foreach { tool =>
          println( "-" * 80)
          println("Command: " + tool.cmd)
          println("Description: " + tool.desc)
          tool.main(Array("--help"))
        }
      } else {
        findTool(args.head) match {
          case Some(tool) => tool.main(args.tail)
          case None => printTools
        }
      }
    }
    //close down the data store and index so the program can exit normally
    Config.persistenceManager.shutdown
    IndexRecords.indexer.shutdown
    System.exit(0)
  }

  def printTools = {
    var idx=1
    tools.foreach{ tool =>
      println( "-" * 80)
      padAndPrint("["+idx+"] " + tool.cmd + " - " + tool.desc )
      idx = idx + 1
    }
  }

  def findTool(str:String) : Option[Tool] = {
    tools.foreach { tool =>
      if(tool.cmd == str) return Some(tool)
    }
    None
  }

  def printTable(table: List[Map[String, String]]) {
    if(table.isEmpty){
      return
    }
    val keys = table(0).keys.toList
    val valueLengths = keys.map(k => {
      (k, table.map(x => x(k).length).max)
    }).toMap[String, Int]
    val columns = table(0).keys.map(k => {
      if (k.length < valueLengths(k)) {
        k + List.fill[String](valueLengths(k) - k.length)(" ").mkString
      } else {
        k
      }
    }).mkString(" | ", " | ", " |")

    val sep = " " + List.fill[String](columns.length - 1)("-").mkString
    println(sep)
    println(columns)
    println(" |" + List.fill[String](columns.length - 3)("-").mkString + "|")

    table.foreach(dr => {
      println(dr.map(kv => {
        if (kv._2.length < valueLengths(kv._1)) {
          kv._2 + List.fill[String](valueLengths(kv._1) - kv._2.length)(" ").mkString
        } else {
          kv._2
        }
      }).mkString(" | ", " | ", " |"))
    })

    println(" " + List.fill[String](columns.length - 1)("-").mkString)
  }

  def padAndPrint(str: String) = println(padElementTo25(str))

  def padElementTo25(str: String) = padElement(str, 25)

  def padElement(str: String, width: Int) = str.replace(" - ", Array.fill(width - str.indexOf(" -"))(' ').mkString + " - ")

  def tools = Array(
    CalculatedLayerHelper,
    ConservationListLoader,
    DataResourceDelete,
    DeleteColumn,
    DeleteRecords,
    DescribeResource,
    DownloadMedia,
    DuplicationDetection,
    DwCACreator,
    DwCALoader,
    DwcCSVLoader,
    DebugRecord,
    ExpertDistributionOutlierTool,
    ExportUtil,
    ExportByFacetQuery,
    ExportFacet,
    ExportForOutliers,
    ExportFromIndex,
    ExportFromIndexStream,
    ExportAllSpatialSpecies,
    GBIFOrgCSVCreator,
    HabitatLoader,
    Healthcheck,
    ImageExport,
    ImportUtil,
    IndexRecords,
    IngestTool,
    IndexMergeTool,
    ListResources,
    Loader,
    OptimiseIndex,
    ProcessAll,
    ProcessSingleRecord,
    ProcessUuids,
    ProcessRecords,
    ReprocessIndexSelect,
    ResampleRecordsByQuery,
    ReloadSampling,
    ResourceCleanupTask,
    Sampling,
    ShowConfig,
    ReverseJacknifeProcessor,
    Thumbnailer,
    BulkProcessor,
    ValidationRuleRunner,
    MigrateMedia,
    LoadMediaReferences,
    CopyDataNewColumn,
    BVPLoader,
    ShowVersion
  ).sortBy(_.cmd)
}
