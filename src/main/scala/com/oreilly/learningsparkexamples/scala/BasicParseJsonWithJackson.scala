/**
 * Illustrates a simple map partition to parse JSON data in Scala
 * Loads the data into a case class with the name and a boolean flag
 * if the person loves pandas.
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature



case class Person(name: String, lovesPandas: Boolean)

object BasicParseJsonWithJackson {

  def main(args: Array[String]) {
    if (args.length < 3) {
      println("Usage: [sparkmaster] [inputfile] [outputfile]")
      exit(1)
      }
    val master = args(0)
    val inputFile = args(1)
    val outputFile = args(2)
    val sc = new SparkContext(master, "BasicParseJsonWithJackson", System.getenv("SPARK_HOME"))
    val input = sc.textFile(inputFile)
    // We use asOpt combined with flatMap so that if it fails to parse we
    // get back a None and the flatMap essentially skips the result.
    val mapper = new ObjectMapper with ScalaObjectMapper
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)
    val result = input.flatMap(record => {
      try {
        val person = mapper.readValue(record, classOf[Person])
        Some(person)
      } catch {
        case e: Exception => None
      }
      })
    result.filter(_.lovesPandas).map(mapper.writeValueAsString(_)).saveAsTextFile(outputFile)
    }
}
