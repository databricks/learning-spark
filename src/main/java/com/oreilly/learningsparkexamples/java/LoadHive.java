/**
 * Illustrates loading data from Hive with Spark SQL
 */
package com.oreilly.learningsparkexamples.java;

import java.io.StringReader;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import scala.Tuple2;

import au.com.bytecode.opencsv.CSVReader;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.JavaSchemaRDD;

public class LoadHive {

  public static class SquareKey implements Function<Row, Integer> {
    public Integer call(Row row) throws Exception {
      return row.getInt(0) * row.getInt(0);
    }
  }

  public static void main(String[] args) throws Exception {
		if (args.length != 3) {
      throw new Exception("Usage LoadHive sparkMaster tbl");
		}
    String master = args[0];
    String tbl = args[1];

		JavaSparkContext sc = new JavaSparkContext(
      master, "loadhive", System.getenv("SPARK_HOME"), System.getenv("JARS"));
    JavaHiveContext hiveCtx = new JavaHiveContext(sc);
    JavaSchemaRDD rdd = hiveCtx.hql("SELECT key, value FROM src");
    JavaRDD<Integer> squaredKeys = rdd.map(new SquareKey());
    List<Integer> result = squaredKeys.collect();
    for (Integer elem : result) {
      System.out.println(elem);
    }
	}
}
