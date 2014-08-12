/**
 * Contains the Chapter 6 Example illustrating accumulators, broadcast variables, numeric operations, and pipe.
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import org.apache.spark.SparkContext._


object AdvancedSparkProgrammingExample {
    def main(args: Array[String]) {
      val master = args(0)
      val inputFile = args(1)
      val outputDir = args(2)
      val sc = new SparkContext(master, "AdvancedSparkProgramming", System.getenv("SPARK_HOME"))
      val file = sc.textFile(inputFile)
      val count = sc.accumulator(0)

      file.foreach(line => {             // side-effecting only
        if (line.contains("KK6JKQ")) {
          count += 1
        }
      })

      println("Lines with 'KK6JKQ': " + count.value)
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
      val validSigns = callSigns.filter{sign =>
        sign match {
          case callSignRegex() => {validSignCount += 1; true}
          case _ => {invalidSignCount += 1; false}
        }
      }
      val contactCount = validSigns.map(callSign => (callSign, 1)).reduceByKey((x, y) => x + y)
      // Force evaluation so the counters are populated
      contactCount.count()
      if (invalidSignCount.value < 0.5 * validSignCount.value) {
        contactCount.saveAsTextFile(outputDir + "/output.txt")
      } else {
        println(s"Too many errors ${invalidSignCount.value} for ${validSignCount.value}")
        exit(1)
      }
      // Lookup the countries for each call sign
      val callSignMap = scala.io.Source.fromFile("./files/callsign_tbl_sorted").getLines().filter(_ != "").map(_.split(",")).toList
      val callSignKeys = sc.broadcast(callSignMap.map(line => line(0)).toArray)
      val callSignLocations = sc.broadcast(callSignMap.map(line => line(1)).toArray)
      val countryContactCount = contactCount.map{case (sign, count) =>
        val pos = java.util.Arrays.binarySearch(callSignKeys.value.asInstanceOf[Array[AnyRef]], sign) match {
          case x if x < 0 => -x-1
          case x => x
        }
        (callSignLocations.value(pos),count)
      }.reduceByKey((x, y) => x + y)
      countryContactCount.saveAsTextFile(outputDir + "/countries.txt")
    }
}
