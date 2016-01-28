/**
 * Illustrates filtering and union to extract lines with "error" or "warning"
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import org.apache.spark.SparkContext._

object BasicFilterUnionCombo {
    def main(args: Array[String]) {
      val conf = new SparkConf
      conf.setMaster(args(0))
      val sc = new SparkContext(conf)
      val inputRDD = sc.textFile(args(1))
      val errorsRDD = inputRDD.filter(_.contains("error"))
      val warningsRDD = inputRDD.filter(_.contains("warn"))
      val badLinesRDD = errorsRDD.union(warningsRDD)
      println(badLinesRDD.collect().mkString("\n"))
    }
}
