/**
 * Illustrates a simple aggregate in scala to compute the average of an RDD
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import org.apache.spark.rdd.RDD

object BasicAvg {
  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "BasicAvg", System.getenv("SPARK_HOME"))
    val input = sc.parallelize(List(1,2,3,4))
    val result = computeAvg(input)
    val avg = result._1 / result._2.toFloat
    println(result)
  }
  def computeAvg(input: RDD[Int]) = {
    input.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1),
      (x,y) => (x._1 + y._1, x._2 + y._2))
  }
}
