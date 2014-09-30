/**
 * Illustrates reading in transfer statistics.
 */
package com.oreilly.learningsparkexamples.scala.logs

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._

import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat


object ReadTransferStats {
  def readStats(ssc: StreamingContext, inputDirectory: String) = {
    ssc.fileStream[LongWritable, IntWritable,
      SequenceFileInputFormat[LongWritable, IntWritable]](inputDirectory)
  }
}
