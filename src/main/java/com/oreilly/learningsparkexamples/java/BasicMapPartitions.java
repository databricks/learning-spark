/**
 * Illustrates a simple map in Java
 */
package com.oreilly.learningsparkexamples.java;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;

import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class BasicMapPartitions {
  public static void main(String[] args) throws Exception {
    String master;
    if (args.length > 0) {
      master = args[0];
    } else {
      master = "local";
    }
    JavaSparkContext sc = new JavaSparkContext(
      master, "basicmappartitions", System.getenv("SPARK_HOME"), System.getenv("JARS"));
    JavaRDD<String> rdd = sc.parallelize(
      Arrays.asList("KK6JKQ", "Ve3UoW", "kk6jlk", "W6BB"));
    JavaRDD<String> result = rdd.mapPartitions(
      new FlatMapFunction<Iterator<String>, String>() {
        public Iterable<String> call(Iterator<String> input) {
          ArrayList<String> content = new ArrayList<String>();
          ArrayList<ContentExchange> cea = new ArrayList<ContentExchange>();
          HttpClient client = new HttpClient();
          try {
            client.start();
            while (input.hasNext()) {
              ContentExchange exchange = new ContentExchange(true);
              exchange.setURL("http://qrzcq.com/call/" + input.next());
              client.send(exchange);
              cea.add(exchange);
            }
            for (ContentExchange exchange : cea) {
              exchange.waitForDone();
              content.add(exchange.getResponseContent());
            }
          } catch (Exception e) {
          }
          return content;
        }});
    System.out.println(StringUtils.join(result.collect(), ","));
  }
}
