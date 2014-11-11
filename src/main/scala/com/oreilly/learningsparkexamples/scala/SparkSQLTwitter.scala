/**
 * Load some tweets stored as JSON data and explore them.
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.hive.HiveContext

case class HappyPerson(handle: String, favouriteBeverage: String)

object SparkSQLTwitter {
    def main(args: Array[String]) {
      if (args.length < 1) {
        println("Usage inputFile outputFile")
      }
      val inputFile = args(0)
      val outputFile = args(1)
      val conf = new SparkConf()
      val sc = new SparkContext(conf)
      val hiveCtx = new HiveContext(sc)
      import hiveCtx._
      // Load some tweets
      val input = hiveCtx.jsonFile(inputFile)
      // Print the schema
      input.printSchema()
      // Register the input schema RDD
      input.registerTempTable("tweets")
      // Select tweets based on the retweetCount
      val topTweets = hiveCtx.hql("SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10")
      topTweets.collect().map(println(_))
      val topTweetText = topTweets.map(row => row.getString(0))
      // Create a person and turn it into a Schema RDD
      val happyPeopleRDD = sc.parallelize(List(HappyPerson("holden", "coffee")))
      happyPeopleRDD.registerTempTable("happy_people")
      // UDF
      registerFunction("strLenScala", (_: String).length)
      val tweetLength = hiveCtx.hql("SELECT strLenScala('tweet') FROM tweets LIMIT 10")
      println(tweetLength.collect())
      sc.stop()
    }
}
