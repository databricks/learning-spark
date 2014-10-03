/**
 * Illustrates a basic Flume stream
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._

object FlumeInput {
  def main(args: Array[String]) {
    val receiverHostname = args(0)
    val receiverPort = args(1).toInt
    val conf = new SparkConf().setAppName("FlumeInput")
    val sc = new SparkContext(conf)
    // Create a StreamingContext with a 1 second batch size
    val ssc = new StreamingContext(sc, Seconds(1))
    val events = FlumeUtils.createStream(ssc, receiverHostname, receiverPort)
    // Assuming that our flume events are UTF-8 log lines
    val lines = events.map{e => new String(e.event.getBody().array(), "UTF-8")}
    lines.print()
    // start our streaming context and wait for it to "finish"
    ssc.start()
    // Wait for 10 seconds then exit. To run forever call without a timeout
    ssc.awaitTermination(10000)
    ssc.stop()
  }
}
