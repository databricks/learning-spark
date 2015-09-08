/**
 * Saves a sequence file of people and how many pandas they have seen.
 */
package com.oreilly.learningsparkexamples.scala

import com.oreilly.learningsparkexamples.proto.Places

import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.hadoop.io.Text
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable
import com.twitter.elephantbird.mapreduce.output.LzoProtobufBlockOutputFormat
import org.apache.hadoop.conf.Configuration

object BasicSaveProtoBuf {
    def main(args: Array[String]) {
      val master = args(0)
      val outputFile = args(1)
      val sc = new SparkContext(master, "BasicSaveProtoBuf", System.getenv("SPARK_HOME"))
      val conf = new Configuration()
      LzoProtobufBlockOutputFormat.setClassConf(classOf[Places.Venue], conf);
      val dnaLounge = Places.Venue.newBuilder()
      dnaLounge.setId(1);
      dnaLounge.setName("DNA Lounge")
      dnaLounge.setType(Places.Venue.VenueType.CLUB)
      val data = sc.parallelize(List(dnaLounge.build()))
      val outputData = data.map{ pb =>
        val protoWritable = ProtobufWritable.newInstance(classOf[Places.Venue]);
        protoWritable.set(pb)
        (null, protoWritable)
      }
      outputData.saveAsNewAPIHadoopFile(outputFile, classOf[Text], classOf[ProtobufWritable[Places.Venue]],
        classOf[LzoProtobufBlockOutputFormat[ProtobufWritable[Places.Venue]]], conf)
    }
}
