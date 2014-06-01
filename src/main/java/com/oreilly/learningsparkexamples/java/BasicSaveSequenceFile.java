/**
 * Illustrates saving a sequence file in Java using the old style hadoop APIs.
 */
package com.oreilly.learningsparkexamples.java;

import java.util.ArrayList;
import java.util.List;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public class BasicSaveSequenceFile {

  public static class ConvertToWritableTypes implements PairFunction<Tuple2<String, Integer>, Text, IntWritable> {
    public Tuple2<Text, IntWritable> call(Tuple2<String, Integer> record) {
      return new Tuple2(new Text(record._1), new IntWritable(record._2));
    }
  }

  public static void main(String[] args) throws Exception {
		if (args.length != 2) {
      throw new Exception("Usage BasicSaveSequenceFile [sparkMaster] [output]");
		}
    String master = args[0];
    String fileName = args[1];

		JavaSparkContext sc = new JavaSparkContext(
      master, "basicloadsequencefile", System.getenv("SPARK_HOME"), System.getenv("JARS"));
    List<Tuple2<String, Integer>> input = new ArrayList();
    input.add(new Tuple2("coffee", 1));
    input.add(new Tuple2("coffee", 2));
    input.add(new Tuple2("pandas", 3));
    JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(input);
    JavaPairRDD<Text, IntWritable> result = rdd.mapToPair(new ConvertToWritableTypes());
    result.saveAsHadoopFile(fileName, Text.class, IntWritable.class, SequenceFileOutputFormat.class);
	}
}
