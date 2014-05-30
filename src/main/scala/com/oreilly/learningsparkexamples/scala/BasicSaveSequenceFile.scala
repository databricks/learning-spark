/**
 * Saves a sequence file of people and how many pandas they have seen.
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import org.apache.spark.SparkContext._

object BasicSaveSequenceFile {
    def main(args: Array[String]) {
      val master = args(0)
      val outputFile = args(1)
      val sc = new SparkContext(master, "BasicSaveSequenceFile", System.getenv("SPARK_HOME"))
      val data = sc.parallelize(List(("Holden", 3), ("Kay", 6), ("Snail", 2)))
      data.saveAsSequenceFile(outputFile)
    }
}
