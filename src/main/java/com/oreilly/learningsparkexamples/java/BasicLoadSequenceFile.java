/**
 * Illustrates loading a sequence file of people and how many pandas they have seen
 */
package com.oreilly.learningsparkexamples.java;

import java.util.List;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class BasicLoadSequenceFile {

  public static class ConvertToNativeTypes implements PairFunction<Tuple2<Text, IntWritable>, String, Integer> {
    public Tuple2<String, Integer> call(Tuple2<Text, IntWritable> record) {
      return new Tuple2(record._1.toString(), record._2.get());
    }
  }

  public static void main(String[] args) throws Exception {
		if (args.length != 2) {
      throw new Exception("Usage BasicLoadSequenceFile [sparkMaster] [input]");
		}
    String master = args[0];
    String fileName = args[1];

		JavaSparkContext sc = new JavaSparkContext(
      master, "basicloadsequencefile", System.getenv("SPARK_HOME"), System.getenv("JARS"));
    JavaPairRDD<Text, IntWritable> input = sc.sequenceFile(fileName, Text.class, IntWritable.class);
    JavaPairRDD<String, Integer> result = input.mapToPair(new ConvertToNativeTypes());
    List<Tuple2<String, Integer>> resultList = result.collect();
    for (Tuple2<String, Integer> record : resultList) {
      System.out.println(record);
    }
	}
}
