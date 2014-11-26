/**
 * Illustrates a simple map then filter in Java
 */
package com.oreilly.learningsparkexamples.java;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.kafka.*;

public final class KafkaInput {
  public static void main(String[] args) throws Exception {
    String zkQuorum = args[0];
    String group = args[1];
    SparkConf conf = new SparkConf().setAppName("KafkaInput");
    // Create a StreamingContext with a 1 second batch size
    JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));
    Map<String, Integer> topics = new HashMap<String, Integer>();
    topics.put("pandas", 1);
    JavaPairDStream<String, String> input = KafkaUtils.createStream(jssc, zkQuorum, group, topics);
    input.print();
    // start our streaming context and wait for it to "finish"
    jssc.start();
    // Wait for 10 seconds then exit. To run forever call without a timeout
    jssc.awaitTermination(10000);
    // Stop the streaming context
    jssc.stop();
	}
}
