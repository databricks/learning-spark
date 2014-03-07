/**
 * Illustrates a simple map then filter in Java
 */
package com.oreilly.learningsparkexamples.java;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class BasicMapThenFilter {
  public static void main(String[] args) throws Exception {
		String master;
		if (args.length > 0) {
      master = args[0];
		} else {
			master = "local";
		}
		JavaSparkContext sc = new JavaSparkContext(
      master, "basicmapfilter", System.getenv("SPARK_HOME"), System.getenv("JARS"));
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    JavaRDD<Integer> squared = rdd.map(
      new Function<Integer, Integer>() { public Integer call(Integer x) { return x*x;}});
    JavaRDD<Integer> result = squared.filter(
      new Function<Integer, Boolean>() { public Boolean call(Integer x) { return x != 1; }});
    System.out.println(StringUtils.join(result.collect(), ","));
	}
}
