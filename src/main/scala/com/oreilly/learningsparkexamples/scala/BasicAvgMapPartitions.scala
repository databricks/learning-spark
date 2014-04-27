/**
 * Illustrates mapPartitions in scala
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._

object BasicAvgMapPartitions {
  case class AvgCount(var total: Int = 0, var num: Int = 0) {
    def merge(other: AvgCount): AvgCount = {
      total += other.total
      num += other.num
      this
    }
    def merge(input: Iterator[Int]): AvgCount = {
      input.foreach{elem =>
        total += elem
        num += 1
      }
      this
    }
    def avg(): Float = {
      total / num.toFloat;
    }
  }

  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "BasicAvgMapPartitions", System.getenv("SPARK_HOME"))
    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.mapPartitions(partition =>
      // Here we only want to return a single element for each partition, but mapPartitions requires that we wrap our return in an Iterator
      Iterator(AvgCount(0, 0).merge(partition)))
      .reduce((x,y) => x.merge(y))
    println(result)
  }
}
