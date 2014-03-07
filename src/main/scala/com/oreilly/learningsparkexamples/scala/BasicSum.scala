/**
 * Illustrates a simple fold in scala
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._

object BasicSum {
    def main(args: Array[String]) {
      val master = args.length match {
        case x: Int if x > 0 => args(0)
        case _ => "local"
      }
      val sc = new SparkContext(master, "BasicMap", System.getenv("SPARK_HOME"))
      val input = sc.parallelize(List(1,2,3,4))
      val result = input.fold(0)((x, y) => (x + y))
      println(result)
    }
}
