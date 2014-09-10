/**
 * Illustrates a wordcount in Java
 */
package com.oreilly.learningsparkexamples.java;

import java.util.Arrays;
import java.util.List;
import java.lang.Iterable;

import scala.Tuple2;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;


public class WordCount {
  public static void main(String[] args) throws Exception {
		String master = args[0];
		JavaSparkContext sc = new JavaSparkContext(
      master, "wordcount", System.getenv("SPARK_HOME"), System.getenv("JARS"));
    JavaRDD<String> rdd = sc.textFile(args[1]);
    JavaPairRDD<String, Integer> counts = rdd.flatMap(
      new FlatMapFunction<String, String>() {
        public Iterable<String> call(String x) {
          return Arrays.asList(x.split(" "));
        }}).mapToPair(new PairFunction<String, String, Integer>(){
            public Tuple2<String, Integer> call(String x){
              return new Tuple2(x, 1);
            }}).reduceByKey(new Function2<Integer, Integer, Integer>(){
                public Integer call(Integer x, Integer y){ return x+y;}});
    counts.saveAsTextFile(args[2]);
	}
}
