/**
 * Illustrates loading JSON data using Spark SQL
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import org.apache.spark.sql.SQLContext


object LoadJsonWithSparkSQL {
  def main(args: Array[String]) {
    if (args.length != 2) {
      println("Usage: [sparkmaster] [inputFile]")
      exit(1)
    }
    val master = args(0)
    val inputFile = args(1)
    val sc = new SparkContext(master, "LoadJsonWithSparkSQL", System.getenv("SPARK_HOME"))
    val sqlCtx = new SQLContext(sc)
    val input = sqlCtx.jsonFile(inputFile)
    input.printSchema()
  }
}
