/**
 * Illustrates flatMap + countByValue for wordcount.
 */
package com.oreilly.learningsparkexamples.mini.scala

import org.apache.spark._
import org.apache.spark.SparkContext._

object WordCount {
    def main(args: Array[String]) {
      val inputFile = args(0)
      val outputFile = args(1)
      val conf = new SparkConf().setAppName("wordCount")
      val sc = new SparkContext(conf)
      // load our input data
      val input =  sc.textFile(inputFile)
      // split it up into words
      val words = input.flatMap(line => line.split(" "))
      // make word and count
      val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
      // save the word count back out to a text file, causing evaluation
      counts.saveAsTextFile(outputFile)
    }
}
