/**
 * Illustrates loading a text file and counting the number of blank lines
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import org.apache.spark.SparkContext._

object AccumulatorExample {
    def main(args: Array[String]) {
      val master = args(0)
      val inputFile = args(1)
      val sc = new SparkContext(master, "AccumulatorExample", System.getenv("SPARK_HOME"))
      val file = sc.textFile(inputFile)
      // Create Accumulator[Int] initialized to 0
      val errorLines = sc.accumulator(0)
      val dataLines = sc.accumulator(0)
      val validSignCount = sc.accumulator(0)
      val invalidSignCount = sc.accumulator(0)
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
          case callSignRegex => {validSignCount += 1; Some(sign)}
          case _ => {invalidSignCount += 1; None}
        }
      }
      val contactCount = validSigns.map((_, 1)).reduceByKey(_ + _)
      if (errorLines.value < 0.1 * dataLines.value) {
        contactCount.saveAsTextFile("output.txt")
      } else {
        println(s"Too many errors ${errorLines.value} for ${dataLines.value}")
      }
    }
}
