/**
 * Illustrates using counters and broadcast variables for chapter 6
 */
package com.oreilly.learningsparkexamples.java;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.*;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

public class ChapterSixExample {
  public static void main(String[] args) throws Exception {

		if (args.length != 3) {
      throw new Exception("Usage AccumulatorExample sparkMaster inputFile outDirectory");
		}
    String sparkMaster = args[0];
    String inputFile = args[1];
    String outputDir = args[2];

    JavaSparkContext sc = new JavaSparkContext(
      sparkMaster, "ChapterSixExample", System.getenv("SPARK_HOME"), System.getenv("JARS"));
    // Create Accumulators initialized at 0
    final Accumulator<Integer> blankLines = sc.accumulator(0);
    JavaRDD<String> rdd = sc.textFile(inputFile);
    JavaRDD<String> callSigns = rdd.flatMap(
      new FlatMapFunction<String, String>() { public Iterable<String> call(String line) {
          if (line.equals("")) {
            blankLines.add(1);
          }
          return Arrays.asList(line.split(" "));
        }});
    callSigns.saveAsTextFile(outputDir + "/callsigns");
    System.out.println("Blank lines: "+ blankLines.value());
    // Start validating the call signs
    final Accumulator<Integer> validSignCount = sc.accumulator(0);
    final Accumulator<Integer> invalidSignCount = sc.accumulator(0);
    JavaRDD<String> validCallSigns = callSigns.filter(
      new Function<String, Boolean>(){ public Boolean call(String callSign) {
          Pattern p = Pattern.compile("\\A\\d\\p{Alpha}{1,2}\\d{1,4}\\{Alpha}{1,3}\\Z");
          Matcher m = p.matcher(callSign);
          boolean b = m.matches();
          if (b) {
            validSignCount.add(1);
          } else {
            invalidSignCount.add(1);
          }
          return b;
        }
      });
    // Force evaluation so the counters are populated
    validCallSigns.count();
    if (invalidSignCount.value() < 0.1 * validSignCount.value()) {
      validCallSigns.saveAsTextFile(outputDir + "/validatedSigns");
    } else {
      System.out.println("Too many errors " + invalidSignCount.value() + " for " + validSignCount.value());
      System.exit(1);
    }
  }
}
