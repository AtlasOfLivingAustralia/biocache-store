package au.org.ala.biocache.util

import java.io.File

import au.org.ala.biocache.Config
import org.apache.commons.io.FileUtils
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryOneTime

case class NodeStatus(processName:String, nodeID:String, status:String, count:Int, lastUpdated:String)

object ZookeeperUtil {

  val RUNNING = "RUNNING"
  val ERROR = "ERROR"
  val COMPLETE = "COMPLETE"
  val UNKNOWN = "UNKNOWN"

  val ZK_NAMESPACE = "biocache"

  var curator:CuratorFramework = null

  private def getCurator : CuratorFramework = {
    if(curator == null){
      val builder = CuratorFrameworkFactory.builder()
      curator = builder
        .retryPolicy(new RetryOneTime(1))
        .connectString(Config.zookeeperAddress).build()
      curator.start()
      curator.usingNamespace(ZK_NAMESPACE)
    }
    curator
  }

  /**
    * Set a running status
    *
    * @param processName
    * @param status
    * @param count
    */
  def setStatus(processName:String, status:String, count:Int): Unit ={
    if(Config.zookeeperUpdatesEnabled) {
      val now = org.apache.commons.lang.time.DateFormatUtils.format(
        new java.util.Date, "yyyy-MM-dd'T'HH:mm:ss'Z'")
      setProperty(processName, Config.nodeNumber.toString, "status", status)
      setProperty(processName, Config.nodeNumber.toString, "count", count.toString)
      setProperty(processName, Config.nodeNumber.toString, "lastUpdated", now)
    }
  }

  /**
    * Set a running status
    *
    * @param processName
    * @param nodeID
    * @param status
    * @param count
    */
  def setStatus(processName:String, nodeID:String, status:String, count:Int): Unit ={
    if(Config.zookeeperUpdatesEnabled) {
      val now = org.apache.commons.lang.time.DateFormatUtils.format(
        new java.util.Date, "yyyy-MM-dd'T'HH:mm:ss'Z'")
      setProperty(processName, nodeID, "status", status)
      setProperty(processName, nodeID, "count", count.toString)
      setProperty(processName, nodeID, "lastUpdated", now)
    }
  }

  /**
   * Set a running status
   *
   * @param processName
   * @param nodeID
   * @param status
   * @param count
   */
  def setStatus(processName:String, nodeID:String, status:String, count:Int, lastUpdated:String): Unit ={
    if(Config.zookeeperUpdatesEnabled) {
      setProperty(processName, nodeID, "status", status)
      setProperty(processName, nodeID, "count", count.toString)
      setProperty(processName, nodeID, "lastUpdated", lastUpdated)
    }
  }

  /**
   * Set a property value.
    *
   * @param processName
   * @param nodeID
   * @param key
   * @param value
   */
  private def setProperty(processName:String, nodeID:String, key:String, value:String): Unit = {
    val stat = getCurator.checkExists().forPath(s"/$processName/$nodeID/$key")
    if(stat == null){
      val createBuilder = getCurator.create().creatingParentsIfNeeded()
      createBuilder.forPath(s"/$processName/$nodeID/$key", value.getBytes)
    } else {
      getCurator.setData().forPath(s"/$processName/$nodeID/$key", value.getBytes)
    }
  }

  private def getProperty(processName:String, nodeID:String, key:String) : Option[String] ={
    val stat = getCurator.checkExists().forPath(s"/$processName/$nodeID/$key")
    if(stat == null){
      None
    } else {
      val data = getCurator.getData().forPath(s"/$processName/$nodeID/$key")
      Some(new String(data).map(_.toChar).toCharArray.mkString)
    }
  }

  def getStatus(processName:String, nodeID:String) : NodeStatus = {
    val status = getProperty(processName, nodeID, "status")
    val count = getProperty(processName, nodeID, "count")
    val lastUpdated = getProperty(processName, nodeID, "lastUpdated")
    NodeStatus(processName, nodeID, status.getOrElse(UNKNOWN), count.getOrElse("-1").toInt, lastUpdated.getOrElse(""))
  }

  /**
    * Retrieve the solr config from zookeeper
    * @param zookeeperHostPort
    * @param solrHome
    */
  def getSolrConfig(zookeeperHostPort:String, solrHome:String): Unit ={

    import scala.collection.JavaConversions._

    println("Reading zookeeper config...")

    val builder = CuratorFrameworkFactory.builder()
    val curator = builder
      .namespace("configs")
      .retryPolicy(new RetryOneTime(1))
      .connectString(zookeeperHostPort).build()

    curator.start()

    val list = curator.getChildren.forPath("/biocache")
    val confDir = new File(solrHome + "/biocache/conf")
    FileUtils.forceMkdir(confDir)
    val dataDir = new File(solrHome + "/biocache/data")
    FileUtils.forceMkdir(dataDir)

    list.foreach(str => {
      val data:Array[Byte] = curator.getData().forPath("/biocache/" + str)
      val configFile = new String(data).map(_.toChar).toCharArray.mkString
      FileUtils.writeStringToFile(new File(solrHome + "/biocache/conf/" + str), configFile)
    })

    FileUtils.writeStringToFile(new File(solrHome + "/solr.xml"), "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><solr></solr>")
    FileUtils.writeStringToFile(new File(solrHome + "/zoo.cfg"), "")
    FileUtils.writeStringToFile(new File(solrHome + "/biocache/core.properties"), "name=biocache\nconfig=solrconfig.xml\nschema=schema.xml\ndataDir=data")
  }
}
