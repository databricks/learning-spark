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
      val rdd = sc.parallelize(Array("i,like,coffee", "also,pandas"))

      // adds our script to a list of files for each node to download with this job
      val splitWords = "/home/holden/repos/learning-spark-examples/src/perl/splitwords.pl"
      sc.addFile(splitWords)

      val piped = rdd.pipe(Seq(SparkFiles.get(splitWords)),
        Map("SEPARATOR" -> ","))
      val result = piped.collect

      println(result.mkString(" "))
    }
}
