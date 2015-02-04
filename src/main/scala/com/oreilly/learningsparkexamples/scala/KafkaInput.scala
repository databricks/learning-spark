/**
 * Illustrates a basic Kafka stream
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object KafkaInput {
  def main(args: Array[String]) {
    val Array(zkQuorum, group, topic, numThreads) = args
    val conf = new SparkConf().setAppName("KafkaInput")
    // Create a StreamingContext with a 1 second batch size
    val ssc = new StreamingContext(conf, Seconds(1))
    // Create a map of topics to number of receiver threads to use
    val topics = List((topic, 1)).toMap
    val topicLines = KafkaUtils.createStream(ssc, zkQuorum, group, topics)
    val lines = StreamingLogInput.processLines(topicLines.map(_._2))
    lines.print()
    // start our streaming context and wait for it to "finish"
    ssc.start()
    // Wait for 10 seconds then exit. To run forever call without a timeout
    ssc.awaitTermination(10000)
    ssc.stop()
  }
}
