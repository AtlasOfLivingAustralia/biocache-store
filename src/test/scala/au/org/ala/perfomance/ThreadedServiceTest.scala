/*
 * Copyright (C) 2012 Atlas of Living Australia
 * All Rights Reserved.
 *
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 */
package au.org.ala.perfomance

import au.org.ala.biocache.util.{FileHelper, StringConsumer, OptionParser}
import java.util.concurrent.ArrayBlockingQueue
import au.org.ala.biocache.util.{StringConsumer, OptionParser, FileHelper}
import org.junit.Ignore
import scala.io.Source
import java.util.concurrent.atomic.AtomicInteger
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

/**
 * Will perform GET operations on the supplied number of threads.  The URLs for the get operations will be
 * extracted from the supplied file. Each URL shoudl be in a separate thread.
 *
 * This is will test any URLs. It may be better to move this to a utilities project.
 */
@Ignore
object ThreadedServiceTest {
  import FileHelper._
  def main(args:Array[String]){
    var numThreads = 8
    var fileName = ""
    val parser = new OptionParser("threaded service test"){
      arg("<url file>", "The absolute path to the file that contains the urls", {v:String => fileName = v})
      intOpt("t","threads", "The number of threads to URL gets on",{v: Int => numThreads =v})
    }
    if(parser.parse(args)){

      val queue = new ArrayBlockingQueue[String](100)
      val ids = new AtomicInteger(0)
      val sentinel = UUID.randomUUID().toString() + System.currentTimeMillis()
      val counter = new AtomicLong(0)
      val error = new AtomicLong(0)
      val startTime = new AtomicLong(System.currentTimeMillis)
      val finishTime = new AtomicLong(0)
      val pool:Array[StringConsumer] = Array.fill(numThreads){
        val id = ids.incrementAndGet()
        val p = new StringConsumer(queue, id, sentinel, {url =>
          val lastCounter = counter.incrementAndGet()
          try {
            Source.fromURL(url)
          } catch {
            case e:Exception => error.incrementAndGet();println(e.getMessage)
          }
          //debug counter
          if (lastCounter % 10 == 0) {
            finishTime.set(System.currentTimeMillis)
            println(id +" >> " + lastCounter +" >> errors >> " + error.get() +" >> average speed for url request " + ((finishTime.get() -startTime.get()).toFloat)/10000f +" seconds " )
            //println(id + " >> " +counter + " >> Last url : " + url + ", records per sec: " + 10f / (((finishTime - startTime).toFloat) / 1000f))
            startTime.set(System.currentTimeMillis)
          }
        });
        p.start;
        p
      }
      new java.io.File(fileName).foreachLine(line =>{
        //add to the queue
        queue.put(line.trim)
      })
      for (i <- 1 to numThreads) {
        queue.put(sentinel)
      }
      pool.foreach(_.join)

    }
  }
}
