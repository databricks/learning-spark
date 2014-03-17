/**
 * Illustrates remove outliers in Java using summary Stats
 */
package com.oreilly.learningsparkexamples.java;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.StatCounter;

public class RemoveOutliers {
  public static void main(String[] args) {
		String master;
		if (args.length > 0) {
      master = args[0];
		} else {
			master = "local";
		}
		JavaSparkContext sc = new JavaSparkContext(
      master, "basicmap", System.getenv("SPARK_HOME"), System.getenv("JARS"));
    JavaDoubleRDD input = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 1000.0));
    JavaDoubleRDD result = removeOutliers(input);
    System.out.println(StringUtils.join(result.collect(), ","));
  }
  static JavaDoubleRDD removeOutliers(JavaDoubleRDD rdd) {
    final StatCounter summaryStats = rdd.stats();
    final Double stddev = Math.sqrt(summaryStats.variance());
    return rdd.filter(new Function<Double, Boolean>() { public Boolean call(Double x) {
          return (Math.abs(x - summaryStats.mean()) < 3 * stddev);
        }});
  }
}
