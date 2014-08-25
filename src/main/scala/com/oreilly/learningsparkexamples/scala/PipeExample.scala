/**
 * Illustrates a simple use of pipe to call a perl program from Spark
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._

object PipeExample {
    def main(args: Array[String]) {
      val master = args.length match {
        case x: Int if x > 0 => args(0)
        case _ => "local"
      }
      val sc = new SparkContext(master, "PipeExample", System.getenv("SPARK_HOME"))
      val rdd = sc.parallelize(Array(
        "37.75889318222431,-122.42683635321838,37.7614213,-122.4240097",
        "37.7519528,-122.4208689,37.8709087,-122.2688365"))

      // adds our script to a list of files for each node to download with this job
      val distScript = "/home/holden/repos/learning-spark-examples/src/R/finddistance.R"
      sc.addFile(distScript)

      val piped = rdd.pipe(Seq(SparkFiles.get(distScript)),
        Map("SEPARATOR" -> ","))
      val result = piped.collect

      println(result.mkString(" "))
    }
}
