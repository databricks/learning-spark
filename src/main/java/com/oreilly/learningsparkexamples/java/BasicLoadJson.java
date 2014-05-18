/**
 * Illustrates loading a json file and finding out if people like pandas
 */
package com.oreilly.learningsparkexamples.java;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.lang.Iterable;
import scala.Tuple2;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.fasterxml.jackson.databind.ObjectMapper;

public class BasicLoadJson {

  public static class Person {
    public String name;
    public Boolean lovesPandas;
  }

  public static class ParseJson implements FlatMapFunction<Iterator<String>, Person> {
    public Iterable<Person> call(Iterator<String> lines) throws Exception {
      ArrayList<Person> people = new ArrayList<Person>();
      ObjectMapper mapper = new ObjectMapper();
      while (lines.hasNext()) {
        String line = lines.next();
        people.add(mapper.readValue(line, Person.class));
      }
      return people;
    }
  }

  public static void main(String[] args) throws Exception {
		if (args.length != 2) {
      throw new Exception("Usage BasicLoadJson [sparkMaster] [jsoninput]");
		}
    String master = args[0];
    String fileName = args[1];
		JavaSparkContext sc = new JavaSparkContext(
      master, "basicloadjson", System.getenv("SPARK_HOME"), System.getenv("JARS"));
    JavaRDD<String> input = sc.textFile(fileName);
    JavaRDD<Person> result = input.mapPartitions(new ParseJson());
    List<Person> resultCollection = result.collect();
	}
}
