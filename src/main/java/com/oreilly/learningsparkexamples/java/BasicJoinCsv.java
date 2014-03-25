/**
 * Illustrates joining two csv files
 */
package com.oreilly.learningsparkexamples.java;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class BasicJoinCsv {
	public String[] pareseLine(String line) throws Exception {
    CSVReader reader = new CSVReader(new StringReader(line));
    String[] line = reader.readNext();
    return line;
	}
  public static void main(String[] args) throws Exception {
		if (args.length != 3) {
      throw new Exception("Usage BasicJoinCsv sparkMaster csv1 csv2")
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
    JavaPairRdd<Integer, String> keyedFile1 = csvFile1.map()
	}
}
