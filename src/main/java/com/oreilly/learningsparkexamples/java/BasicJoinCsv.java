/**
 * Illustrates joining two csv files
 */
package com.oreilly.learningsparkexamples.java;

import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import scala.Tuple2;

import au.com.bytecode.opencsv.CSVReader;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

public class BasicJoinCsv {

  public static class ParseLine implements PairFunction<String, Integer, String[]> {
    public Tuple2<Integer, String[]> call(String line) throws Exception {
      CSVReader reader = new CSVReader(new StringReader(line));
      String[] elements = reader.readNext();
      Integer key = Integer.parseInt(elements[0]);
      return new Tuple2(key, elements);
    }
  }

  public static void main(String[] args) throws Exception {
		if (args.length != 3) {
      throw new Exception("Usage BasicJoinCsv sparkMaster csv1 csv2");
		}
    String master = args[0];
    String csv1 = args[1];
    String csv2 = args[2];
    BasicJoinCsv jsv = new BasicJoinCsv();
    jsv.run(master, csv1, csv2);
  }

  public void run(String master, String csv1, String csv2) throws Exception {
		JavaSparkContext sc = new JavaSparkContext(
      master, "basicjoincsv", System.getenv("SPARK_HOME"), System.getenv("JARS"));
    JavaRDD<String> csvFile1 = sc.textFile(csv1);
    JavaRDD<String> csvFile2 = sc.textFile(csv2);
    JavaPairRDD<Integer, String[]> keyedRDD1 = csvFile1.mapToPair(new ParseLine());
    JavaPairRDD<Integer, String[]> keyedRDD2 = csvFile1.mapToPair(new ParseLine());
    JavaPairRDD<Integer, Tuple2<String[], String[]>> result = keyedRDD1.join(keyedRDD2);
    List<Tuple2<Integer, Tuple2<String[], String[]>>> resultCollection = result.collect();
	}
}
