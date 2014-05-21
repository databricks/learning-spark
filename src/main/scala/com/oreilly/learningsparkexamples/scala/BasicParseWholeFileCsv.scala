/**
 * Illustrates a simple map partition to parse CSV data in Scala
 */
package com.oreilly.learningsparkexamples.scala

import java.io.StringReader

import org.apache.spark._
import play.api.libs.json._
import play.api.libs.functional.syntax._
import scala.util.parsing.json.JSON
import scala.collection.JavaConversions._
import au.com.bytecode.opencsv.CSVReader

object BasicParseWholeFileCsv {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: [sparkmaster] [inputfile]")
      exit(1)
    }
    val master = args(0)
    val inputFile = args(1)
    val sc = new SparkContext(master, "BasicParseWholeFileCsv", System.getenv("SPARK_HOME"))
    val input = sc.wholeTextFiles(inputFile)
    val result = input.flatMap{ case (_, txt) =>
      val reader = new CSVReader(new StringReader(txt));
      reader.readAll()
    }
    println(result.collect().map(_.toList).mkString(","))
    }
}
