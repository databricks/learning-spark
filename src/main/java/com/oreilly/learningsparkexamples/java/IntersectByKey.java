/**
 * Illustrates a simple map in Java
 */
package com.oreilly.learningsparkexamples.java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Iterables;

import scala.Tuple2;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.FlatMapFunction;

public final class IntersectByKey {
  public static <K, V> JavaPairRDD<K, V> intersectByKey(JavaPairRDD<K, V> rdd1, JavaPairRDD<K, V> rdd2) {
    JavaPairRDD<K, Tuple2<Iterable<V>, Iterable<V>>> grouped = rdd1.cogroup(rdd2);
    return grouped.flatMapValues(new Function<Tuple2<Iterable<V>, Iterable<V>>, Iterable<V>>() {
        @Override
        public Iterable<V> call(Tuple2<Iterable<V>, Iterable<V>> input) {
          ArrayList<V> al = new ArrayList<V>();
          if (!Iterables.isEmpty(input._1()) && !Iterables.isEmpty(input._2())) {
            Iterables.addAll(al, input._1());
            Iterables.addAll(al, input._2());
          }
          return al;
        }
        });
  }
  public static void main(String[] args) throws Exception {
		String master;
		if (args.length > 0) {
      master = args[0];
		} else {
			master = "local";
		}

		JavaSparkContext sc = new JavaSparkContext(
      master, "IntersectByKey", System.getenv("SPARK_HOME"), System.getenv("JARS"));
    List<Tuple2<String, Integer>> input1 = new ArrayList();
    input1.add(new Tuple2("coffee", 1));
    input1.add(new Tuple2("coffee", 2));
    input1.add(new Tuple2("pandas", 3));
    List<Tuple2<String, Integer>> input2 = new ArrayList();
    input2.add(new Tuple2("pandas", 20));
    JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(input1);
    JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(input2);
    JavaPairRDD<String, Integer> result = intersectByKey(rdd1, rdd2);
    for (Tuple2<String, Integer> entry : result.collect()) {
      System.out.println(entry._1() + ":" + entry._2());
    }
    System.out.println("Done");
    sc.stop();
	}
}
