/**
 * Load some tweets stored as JSON data and explore them.
 */
package com.oreilly.learningsparkexamples.java;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

public class SparkSQLTwitter {
  public static void main(String[] args) {
    String inputFile = args[0];
    SparkConf conf = new SparkConf();
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaSQLContext sqlCtx = new JavaSQLContext(sc);
    JavaSchemaRDD input = sqlCtx.jsonFile(inputFile);
    // Print the schema
    input.printSchema();
    // Register the input schema RDD
    input.registerTempTable("tweets");
    // Select tweets based on the retweetCount
    JavaSchemaRDD topTweets = sqlCtx.sql("SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10");
    List<Row> result = topTweets.collect();
    for (Row row : result) {
      System.out.println(row.get(0));
    }
    sc.stop();
  }
}
