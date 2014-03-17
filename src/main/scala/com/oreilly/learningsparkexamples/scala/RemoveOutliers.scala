/**
 * Illustrates remove outliers in Scala using summary Stats
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object RemoveOutliers {
  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "RemoveOutliers", System.getenv("SPARK_HOME"))
    val input = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1000)).map(_.toDouble)
    val result = removeOutliers(input)
    println(result.collect().mkString(","))
  }
  def removeOutliers(rdd: RDD[Double]): RDD[Double] = {
    val summaryStats = rdd.stats()
    val stddev = math.sqrt(summaryStats.variance)
    rdd.filter(x => math.abs(x-summaryStats.mean) < 3 * stddev)
  }
}
