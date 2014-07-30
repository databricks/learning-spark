/**
 * Illustrates using counters and broadcast variables for chapter 6
 */
package com.oreilly.learningsparkexamples.java;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


import org.apache.commons.lang.StringUtils;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class ChapterSixExample {
  public static void main(String[] args) throws Exception {

		if (args.length != 3) {
      throw new Exception("Usage AccumulatorExample sparkMaster inputFile outDirectory");
		}

    JavaSparkContext sc = new JavaSparkContext(
      args[0], "ChapterSixExample", System.getenv("SPARK_HOME"), System.getenv("JARS"));
    // Create Accumulators initialized at 0
    final Accumulator<Integer> blankLines = sc.accumulator(0);
    JavaRDD<String> rdd = sc.textFile(args[1]);
    JavaRDD<String> callSigns = rdd.flatMap(
      new FlatMapFunction<String, String>() { public Iterable<String> call(String line) {
          if (line.equals("")) {
            blankLines.add(1);
          }
          return Arrays.asList(line.split(" "));
        }});
    // Force evaluation so the counters are populated
    callSigns.count();
    System.out.println("Blank lines: "+ blankLines.value());
  }
}
