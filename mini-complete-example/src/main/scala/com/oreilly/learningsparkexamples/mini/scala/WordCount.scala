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
      val input =  sc.textFile(inputFile)
      val words = input.flatMap(line => line.split(" "))
      val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
      counts.saveAsTextFile(outputFile)
    }
}
