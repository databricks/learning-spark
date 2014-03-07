/**
 * Illustrates a simple map the filter in Scala
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._

object BasicMapThenFilter {
    def main(args: Array[String]) {
      val master = args.length match {
        case x: Int if x > 0 => args(0)
        case _ => "local"
      }
      val sc = new SparkContext(master, "BasicMap", System.getenv("SPARK_HOME"))
      val input = sc.parallelize(List(1,2,3,4))
      val squared = input.map(x => x*x)
      val result = squared.filter(x => x != 1)
      println(result.collect().mkString(","))
    }
}
