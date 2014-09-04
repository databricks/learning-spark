/**
 * Contains the Chapter 6 Example illustrating accumulators, broadcast variables, numeric operations, and pipe.
 */
package com.oreilly.learningsparkexamples.scala

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature

import org.apache.spark._
import org.apache.spark.SparkContext._

import org.eclipse.jetty.client.ContentExchange
import org.eclipse.jetty.client.HttpClient

case class QSO(callsign: String="", contactlat: Option[Double]=None,
  contactlong: Option[Double]=None, mylat: Option[Double]=None, mylong: Option[Double]=None)

object ChapterSixExample {
  def main(args: Array[String]) {
    val master = args(0)
    val inputFile = args(1)
    val inputFile2 = args(2)
    val outputDir = args(3)
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
      if ((callSignRegex findFirstIn sign).nonEmpty) {
        validSignCount += 1; true
      } else {
        invalidSignCount += 1; false
      }
    }
    val contactCounts = validSigns.map(callSign => (callSign, 1)).reduceByKey((x, y) => x + y)
    // Force evaluation so the counters are populated
    contactCounts.count()
    if (invalidSignCount.value < 0.5 * validSignCount.value) {
      contactCounts.saveAsTextFile(outputDir + "/output.txt")
    } else {
      println(s"Too many errors ${invalidSignCount.value} for ${validSignCount.value}")
      exit(1)
    }
    // Lookup the countries for each call sign for the
    // contactCounts RDD
    val signPrefixes = sc.broadcast(loadCallSignTable())
    val countryContactCounts = contactCounts.map{case (sign, count) =>
      val country = lookupInArray(sign, signPrefixes.value)
      (country, count)
    }.reduceByKey((x, y) => x + y)
    countryContactCounts.saveAsTextFile(outputDir + "/countries.txt")
    // Resolve call signs in a second file to location
    val countryCounts2 = sc.textFile(inputFile2)
      .flatMap(_.split("\\s+"))      // Split line into words
      .map{case sign =>
        val country = lookupInArray(sign, signPrefixes.value)
        (country, 1)}.reduceByKey((x, y) => x + y).collect()
    // Look up the location info using a connection pool
    val contactsContactList = validSigns.distinct().mapPartitions{
      signs =>
      val mapper = createMapper()
      val client = new HttpClient()
      client.start()
      signs.map {sign =>
        val exchange = new ContentExchange(true);
        exchange.setURL(s"http://new73s.herokuapp.com/qsos/${sign}.json")
        client.send(exchange)
        (sign, exchange)
      }.map{ case (sign, exchange) =>
          exchange.waitForDone();
          val responseJson = exchange.getResponseContent()
          try {
            val qsos = mapper.readValue(responseJson, classOf[Array[QSO]])
            (sign, qsos)
          } catch {
            case e: Exception => null
          }
      }.filter( x => x != null)
    }
    println(contactsContactList.collect().toList)
    // Computer the distance of each call using an external R program
    // adds our script to a list of files for each node to download with this job
    val distScript = "./src/R/finddistance.R"
    val distScriptName = "finddistance.R"
    sc.addFile(distScript)
    val distance = contactsContactList.values.flatMap(x => x.map(y =>
      s"$y.contactlay,$y.contactlong,$y.mylat,$y.mylong")).pipe(Seq(
        SparkFiles.get(distScriptName)),
        Map("SEPARATOR" -> ","))
    // Now we can go ahead and remove outliers since those may have misreported locations
    // first we need to take our RDD of strings and turn it into doubles.
    val distanceDouble = distance.map(string => string.toDouble)
    val stats = distanceDouble.stats()
    val stddev = stats.stdev
    val mean = stats.mean
    val reasonableDistance = distanceDouble.filter(x => math.abs(x-mean) < 3 * stddev)
    println(reasonableDistance.collect().toList)
  }

  def lookupInArray(sign: String, prefixArray: Array[String]): String = {
    val pos = java.util.Arrays.binarySearch(prefixArray.asInstanceOf[Array[AnyRef]], sign) match {
      case x if x < 0 => -x-1
      case x => x
    }
    // The country is the second element separated by comma
    prefixArray(pos).split(",")(1)
  }

  def loadCallSignTable() = {
    scala.io.Source.fromFile("./files/callsign_tbl_sorted").getLines()
      .filter(_ != "").toArray
  }

  def createMapper() = {
    val mapper = new ObjectMapper with ScalaObjectMapper
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)
    mapper
  }
}
