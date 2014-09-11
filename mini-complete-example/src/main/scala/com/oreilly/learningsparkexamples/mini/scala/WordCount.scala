/**
 * Illustrates flatMap + countByValue for wordcount.
 */
package com.oreilly.learningsparkexamples.mini.scala

import org.apache.spark._
import org.apache.spark.SparkContext._

object WordCount {
    def main(args: Array[String]) {
      val master = args(0)
      val inputFile = args(1)
      val outputFile = args(2)
      val sc = new SparkContext(master, "WordCount", System.getenv("SPARK_HOME"))
      // load our input data
      val input =  sc.textFile(inputFile)
      // split it up into lines
      val words = input.flatMap(line => line.split(" "))
      // make word and count
      val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
      // save the word count back out to a text file, causing evaluation
      counts.saveAsTextFile(outputFile)
    }
}
