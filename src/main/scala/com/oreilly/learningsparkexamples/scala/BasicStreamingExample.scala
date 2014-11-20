/**
 * A sample streaming application saves the wordcounts of a specific window in time
 */

package com.oreilly.learningsparkexamples.scala

import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark._
import org.apache.spark.SparkContext._


object BasicStreamingExample {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage BasicStreamingExample <master> <output>")
    }
    val Array(master, output) = args.take(2)

    val conf = new SparkConf().setMaster(master).setAppName("BasicStreamingExample")
    val ssc = new StreamingContext(conf, Seconds(30))

    val lines = ssc.socketTextStream("localhost" , 7777)
    val words = lines.flatMap(_.split(" "))
    val wc = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)

    wc.saveAsTextFiles(output)
    wc.print

    println("pandas: sscstart")
    ssc.start()
    println("pandas: awaittermination")
    ssc.awaitTermination()
    println("pandas: done!")
  }
}
