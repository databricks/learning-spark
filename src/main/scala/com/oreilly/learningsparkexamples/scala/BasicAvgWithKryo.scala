/**
 * Illustrates a simple fold in scala
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._

object BasicAvgWithKryo {
    def main(args: Array[String]) {
      val master = args.length match {
        case x: Int if x > 0 => args(0)
        case _ => "local"
      }
      val conf = new SparkConf().setMaster(master).setAppName("basicAvgWithKryo")
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      val sc = new SparkContext(conf)
      val input = sc.parallelize(List(1,2,3,4))
      val result = input.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1),
        (x,y) => (x._1 + y._1, x._2 + y._2))
      val avg = result._1 / result._2.toFloat
      println(result)
    }
}
