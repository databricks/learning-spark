/**
 * Contains the Chapter 6 Example
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import org.apache.spark.SparkContext._


object AdvancedSparkProgrammingExample {
    def main(args: Array[String]) {
      val master = args(0)
      val inputFile = args(1)
      val sc = new SparkContext(master, "AdvancedSparkProgramming", System.getenv("SPARK_HOME"))
      val file = sc.textFile(inputFile)
      // Create Accumulator[Int] initialized to 0
      val errorLines = sc.accumulator(0)
      val dataLines = sc.accumulator(0)
      val validSignCount = sc.accumulator(0)
      val invalidSignCount = sc.accumulator(0)
      val unknownCountry = sc.accumulator(0)
      val resolvedCountry = sc.accumulator(0)
      val callSigns = file.flatMap(line => {
        if (line == "") {
          errorLines += 1
        } else {
          dataLines +=1
        }
        line.split(" ")
      })
      // Validate a call sign
      val callSignRegex = "\\A\\d?[a-zA-Z]{1,2}\\d{1,4}[a-zA-Z]{1,3}\\Z".r
      val validSigns = callSigns.flatMap{sign =>
        sign match {
          case callSignRegex() => {validSignCount += 1; Some(sign)}
          case _ => {invalidSignCount += 1; None}
        }
      }
      val contactCount = validSigns.map(callSign => (callSign, 1)).reduceByKey((x, y) => x + y)
      if (errorLines.value < 0.1 * dataLines.value) {
        contactCount.saveAsTextFile("output.txt")
      } else {
        println(s"Too many errors ${errorLines.value} for ${dataLines.value}")
        exit(1)
      }
      // Lookup the countries for each call sign
      val callSignMap = scala.io.Source.fromFile("./files/callsign_tbl").getLines().map(line => line.span(x => x == ','))
      val prefixRanges = scala.collection.immutable.TreeMap(callSignMap.toSeq:_*)
      val countryContactCount = contactCount.map{case (sign, count) =>
        (prefixRanges.rangeImpl(Some(sign), None).head._2,count)
      }.reduceByKey((x, y) => x + y)
      if (unknownCountry.value < 0.1 * resolvedCountry.value) {
        contactCount.saveAsTextFile("countries.txt")
      } else {
        println(s"Too many unresolved countries ${unknownCountry.value} for ${resolvedCountry.value} resolved countries")
      }

    }
}
