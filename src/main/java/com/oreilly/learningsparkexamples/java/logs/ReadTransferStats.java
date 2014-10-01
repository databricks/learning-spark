package com.oreilly.learningsparkexamples.java.logs;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;


import java.io.Serializable;

public class ReadTransferStats implements Serializable {

  public JavaPairDStream<Long, Integer> readStats(JavaStreamingContext jssc, String inputDirectory) {
    // Note: This example doesn't work until Spark 1.2
    JavaPairDStream<LongWritable, IntWritable> input = jssc.fileStream(inputDirectory);
    // convert the input from Writables to native types
    JavaPairDStream<Long, Integer> usefulInput = input.mapToPair(
      new PairFunction<Tuple2<LongWritable, IntWritable>, Long, Integer>() {
        public Tuple2<Long, Integer> call(Tuple2<LongWritable, IntWritable> input) {
          return new Tuple2(input._1().get(), input._2().get());
        }
      });
    return usefulInput;
  }

}
