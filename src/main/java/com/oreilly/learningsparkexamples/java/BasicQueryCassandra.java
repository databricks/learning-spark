/**
 * Illustrates loading a json file and finding out if people like pandas
 */
package com.oreilly.learningsparkexamples.java;
import java.io.Serializable;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.lang.Iterable;
import scala.Tuple2;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import com.datastax.spark.connector.CassandraRow;
import static com.datastax.spark.connector.CassandraJavaUtil.javaFunctions;

public class BasicQueryCassandra {
  public static void main(String[] args) throws Exception {
		if (args.length != 2) {
      throw new Exception("Usage BasicLoadJson [sparkMaster] [cassandraHost]");
		}
    String sparkMaster = args[0];
    String cassandraHost = args[1];
    SparkConf conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", cassandraHost);

		JavaSparkContext sc = new JavaSparkContext(
      sparkMaster, "basicquerycassandra", conf);
    // entire table as an RDD
    // assumes your table test was created as CREATE TABLE test.kv(key text PRIMARY KEY, value int);
    JavaRDD<CassandraRow> data = javaFunctions(sc).cassandraTable("test" , "kv");
    // print some basic stats
    System.out.println(data.mapToDouble(new DoubleFunction<CassandraRow>() {
        public double call(CassandraRow row) {
          return row.getInt("value");
        }}).stats());
    // write some basic data to Cassandra
    ArrayList<KeyValue> input = new ArrayList<KeyValue>();
    input.add(KeyValue.newInstance("mostmagic", 3));
    JavaRDD<KeyValue> kvRDD = sc.parallelize(input);
    javaFunctions(kvRDD, KeyValue.class).saveToCassandra("test", "kv");
	}
  public static class KeyValue implements Serializable {
    private String key;
    private Integer value;
    public KeyValue() {
    }
    public static KeyValue newInstance(String k, Integer v) {
      KeyValue kv = new KeyValue();
      kv.setKey(k);
      kv.setValue(v);
      return kv;
    }
    public String getKey() {
      return key;
    }
    public Integer getValue() {
      return value;
    }
    void setKey(String k) {
      this.key = k;
    }
    void setValue(Integer v) {
      this.value = v;
    }
  }
}

