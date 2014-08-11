/**
 * Illustrates using counters and broadcast variables for chapter 6
 */
package com.oreilly.learningsparkexamples.java;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.*;
import java.util.Scanner;
import java.io.File;

import scala.Tuple2;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.Accumulator;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

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
    JavaRDD<String> rdd = sc.textFile(inputFile);
    // Count the number of lines with KK6JKQ
    final Accumulator<Integer> count = sc.accumulator(0);
    rdd.foreach(new VoidFunction<String>(){ public void call(String line) {
          if (line.contains("KK6JKQ")) {
            count.add(1);
          }
        }});
    System.out.println("Lines with 'KK6JKQ': " + count.value());
    // Create Accumulators initialized at 0
    final Accumulator<Integer> blankLines = sc.accumulator(0);
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
    JavaPairRDD<String, Integer> contactCount = validCallSigns.mapToPair(
      new PairFunction<String, String, Integer>() {
        public Tuple2<String, Integer> call(String callSign) {
          return new Tuple2(callSign, 1);
        }}).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) {
              return x + y;
            }});
    // Force evaluation so the counters are populated
    contactCount.count();
    if (invalidSignCount.value() < 0.1 * validSignCount.value()) {
      contactCount.saveAsTextFile(outputDir + "/contactCount");
    } else {
      System.out.println("Too many errors " + invalidSignCount.value() + " for " + validSignCount.value());
      System.exit(1);
    }
    // Read in the call sign table
    Scanner callSignTbl = new Scanner(new File("./files/callsign_tbl_sorted"));
    ArrayList<String> callSignList = new ArrayList<String>();
    while (callSignTbl.hasNextLine()) {
      callSignList.add(callSignTbl.nextLine());
    }
    final Broadcast<String[]> callSignsMap = sc.broadcast(callSignList.toArray(new String[0]));
    validCallSigns.mapToPair(
      new PairFunction<Tuple2<String, Integer>, String, Integer> (){
        public Tuple2<String, Integer> call(Tuple2<String, Integer> callSignCount) {
          String[] callSignInfo = callSignsMap.value();
          String sign = callSignCount._1();
          Integer pos = java.util.Arrays.binarySearch(callSignInfo, sign);
          if (pos < 0) {
            pos = -pos-1;
          }
          return new Tuple2(callSignInfo[pos], callSignCount._2());
        }});
  }
}
