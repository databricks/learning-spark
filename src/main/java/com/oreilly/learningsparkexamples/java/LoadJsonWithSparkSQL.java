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
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.JavaSchemaRDD;

public class LoadJsonWithSparkSQL {


  public static void main(String[] args) throws Exception {
		if (args.length != 2) {
      throw new Exception("Usage LoadJsonWithSparkSQL sparkMaster jsonFile");
		}
    String master = args[0];
    String jsonFile = args[1];

		JavaSparkContext sc = new JavaSparkContext(
      master, "loadJsonwithsparksql");
    JavaSQLContext sqlCtx = new JavaSQLContext(sc);
    JavaSchemaRDD input = sqlCtx.jsonFile(jsonFile);
    input.printSchema();
  }
}
