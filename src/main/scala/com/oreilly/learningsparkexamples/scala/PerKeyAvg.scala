/**
 * Illustrates a simple fold in scala
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import org.apache.spark.SparkContext._

object PerKeyAvg {
    def main(args: Array[String]) {
      val master = args.length match {
        case x: Int if x > 0 => args(0)
        case _ => "local"
      }

      val sc = new SparkContext(master, "PerKeyAvg", System.getenv("SPARK_HOME"))
      val input = sc.parallelize(List(("coffee", 1) , ("coffee", 2) , ("panda", 4)))
      val result = input.combineByKey(
        (v) => (v, 1),
        (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
        (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
      // Note: we could us mapValues here, but we didn't because it was in the next section
      ).map{ case (key, value) => (key, value._1 / value._2.toFloat) }
      result.collectAsMap().map(println(_))
    }
}
