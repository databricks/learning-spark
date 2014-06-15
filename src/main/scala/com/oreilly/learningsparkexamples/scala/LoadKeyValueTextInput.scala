/**
 * Illustrates a simple map partition to parse JSON data in Scala
 * Loads the data into a case class with the name and a boolean flag
 * if the person loves pandas.
 */
package com.oreilly.learningsparkexamples.scala

import scala.collection.JavaConversions._
import org.apache.spark._
import org.apache.hadoop.mapred.KeyValueTextInputFormat
import org.apache.hadoop.io.{MapWritable, Text}
import java.util.HashMap

object LoadKeyValueTextInput {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: [sparkmaster] [inputfile]")
      exit(1)
    }
    val master = args(0)
    val inputFile = args(1)
    val sc = new SparkContext(master, "LoadKeyValueTextInput", System.getenv("SPARK_HOME"))
    val input = sc.hadoopFile[Text, Text, KeyValueTextInputFormat](inputFile).map{
      case (x, y) => (x.toString, y.toString)
    }
    println(input.collect().toList)
    }
}
