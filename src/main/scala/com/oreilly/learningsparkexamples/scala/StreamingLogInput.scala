/**
 * Illustrates a simple streaming application
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._

object StreamingLogInput {
  def main(args: Array[String]) {
    val master = args(0)
    val conf = new SparkConf().setMaster(master).setAppName("StreamingLogInput")
    // Create a StreamingContext with a 1 second batch size
    val ssc = new StreamingContext(conf, Seconds(1))
    // Create a DStream from all the input on port 7777
    val lines = ssc.socketTextStream("localhost", 7777)
    val errorLines = processLines(lines)
    // Print out the lines with errors, which causes this DStream to be evaluated
    errorLines.print()
    // start our streaming context and wait for it to "finish"
    ssc.start()
    // Wait for 10 seconds then exit. To run forever call without a timeout
    ssc.awaitTermination(10000)
    ssc.stop()
  }
  def processLines(lines: DStream[String]) = {
    // Filter our DStream for lines with "error"
    lines.filter(_.contains("error"))
  }

}
