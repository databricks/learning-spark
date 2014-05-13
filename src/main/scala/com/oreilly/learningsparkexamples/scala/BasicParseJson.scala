/**
 * Illustrates a simple map partition to parse JSON data in Scala
 * Loads the data into a case class with the name and a boolean flag
 * if the person loves pandas.
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import play.api.libs.json._
import play.api.libs.functional.syntax._
import scala.util.parsing.json.JSON

object BasicParseJson {
  case class Person(name: String, lovesPandas: Boolean)
  implicit val personReads = Json.format[Person]

  def main(args: Array[String]) {
      if (args.length < 2) {
        println("Usage: [sparkmaster] [inputfile]")
        exit(1)
      }
      val master = args(0)
      val inputFile = args(1)
      val sc = new SparkContext(master, "BasicParseJson", System.getenv("SPARK_HOME"))
      val input = sc.textFile(inputFile)
      val parsed = input.map(JSON.parseFull(_))
      val result = parsed.map(record => personReads.reads(_))

      println(result.collect().mkString(","))
    }
}
