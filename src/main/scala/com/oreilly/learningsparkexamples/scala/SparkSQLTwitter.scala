/**
 * Load some tweets stored as JSON data and explore them.
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext


object SparkSQLTwitter {
    def main(args: Array[String]) {
      val conf = new SparkConf()
      val sc = new SparkContext(conf)
      val sqlCtx = new SQLContext(sc)
      import sqlCtx._
    }
}
