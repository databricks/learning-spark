package com.oreilly.learningsparkexamples.scala.logs;

import org.apache.spark._
import org.apache.spark.streaming._

/**
 * The LogAnalyzerAppMain is an sample logs analysis application.  For now,
 * it is a simple minimal viable product:
 *   - Read in new log files from a directory and input those new files into streaming.
 *   - Computes stats for all of time as well as the last time interval based on those logs.
 *   - Write the calculated stats to an txt file on the local file system
 *     that gets refreshed every time interval.
 *
 * Once you get this program up and running, feed apache access log files
 * into the local directory of your choosing.
 *
 * Then open your output text file, perhaps in a web browser, and refresh
 * that page to see more stats come in.
 *
 * Modify the command line flags to the values of your choosing.
 * Notice how they come after you specify the jar when using spark-submit.
 *
 * Example command to run:
 * %  ${YOUR_SPARK_HOME}/bin/spark-submit
 *     --class "com.oreilly.learningsparkexamples.scala.logs.LogAnalyzerAppMain"
 *     --master local[4]
 *     target/uber-log-analyzer-1.0.jar
 *     --logs_directory /tmp/logs
 *     --output_html_file /tmp/log_stats.html
 *     --index_html_template ./src/main/resources/index.html.template
 */
object LogAnalyzerAppMain {
  case class Config(WindowLength: Int = -1, SlideInterval: Int = -1, LogsDirectory: String = "/tmp/logs",
    OutputHTMLFile: String = "/tmp/log_stats.html",
    IndexHTMLTemplate :String ="./src/main/resources/index.html.template")

  def main() {
    val parser = new scopt.OptionParser[Config]("LogAnalyzerAppMain") {}

  }
}
