/**
 * Illustrates loading a directory of files
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import org.apache.spark.SparkContext._

object BasicAvgFromFiles {
    def main(args: Array[String]) {
      if (args.length < 3) {
        println("Usage: [sparkmaster] [inputdirectory] [outputdirectory]")
        exit(1)
      }
      val master = args(0)
      val inputFile = args(1)
      val outputFile = args(2)
      val sc = new SparkContext(master, "BasicAvgFromFiles", System.getenv("SPARK_HOME"))
      val input = sc.wholeTextFiles(inputFile)
      val result = input.mapValues{y =>
        val nums = y.split(" ").map(_.toDouble)
        nums.sum / nums.size.toDouble
      }
      result.saveAsTextFile(outputFile)
    }
}
