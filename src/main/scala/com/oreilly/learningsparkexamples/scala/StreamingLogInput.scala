/**
 * Illustrates a simple streaming application
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._

object StreamingLogInput {
    def main(args: Array[String]) {
      val master = args(0)
      val sc = new SparkContext(master, "StreamingLogInput", System.getenv("SPARK_HOME"))
      // Create a StreamingContext with a 1 second batch size
      val ssc = new StreamingContext(sc, Seconds(1))
      // Create a DStream from all the input on port 7777
      val lines = ssc.socketTextStream("localhost", 7777)
      // Filter our DStream for lines with "error"
      val errorLines = lines.filter(_.contains("error"))
      // Print out the lines with errors, which causes this DStream to be evaluated
      errorLines.print()
    }
}
