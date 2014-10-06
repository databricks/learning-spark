package com.oreilly.learningsparkexamples.scala.logs;

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import com.oreilly.learningsparkexamples.java.logs.ApacheAccessLog

/**
 * Compute totals on the log input
 */
object LogAnalyzerTotal {
  def processAccessLogs(accessLogsDStream: DStream[ApacheAccessLog]) {
    val ipAddressesRawDStream = accessLogsDStream.transform{
      rdd => rdd.map(accessLog => (accessLog.getIpAddress(), 1)).reduceByKey(
        (x, y) => x +y)
    }
    ipAddressesRawDStream.print()
  }
}
