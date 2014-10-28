package com.oreilly.learningsparkexamples.scala.logs;

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream._
import com.oreilly.learningsparkexamples.java.logs.ApacheAccessLog

/**
 * Computes various pieces of information on a sliding window form the log input
 */
object LogAnalyzerWindowed {
  def responseCodeCount(accessLogRDD: RDD[ApacheAccessLog]) = {
    accessLogRDD.map(log => (log.getResponseCode(), 1)).reduceByKey((x, y) => x + y)
  }

  def processAccessLogs(accessLogsDStream: DStream[ApacheAccessLog], opts: Config) {
    val ipAddressesDStream = accessLogsDStream.map{entry => entry.getIpAddress()}
    val ipAddressRequestCount = ipAddressesDStream.countByValueAndWindow(
      opts.getWindowDuration(), opts.getSlideDuration())
    ipAddressRequestCount.saveAsTextFiles(opts.OutputDirectory + "/ipAddressRequestCountsTXT")
    val requestCount = accessLogsDStream.countByWindow(opts.getWindowDuration(), opts.getSlideDuration())
    requestCount.print()
    ipAddressRequestCount.print()
    val accessLogsWindow = accessLogsDStream.window(
      opts.getWindowDuration(), opts.getSlideDuration())
    accessLogsWindow.transform(rdd => responseCodeCount(rdd)).print()
  }
}
