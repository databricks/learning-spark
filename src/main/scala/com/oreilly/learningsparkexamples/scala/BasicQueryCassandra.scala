/**
 * A simple illustration of querying Cassandra
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
// Implicits that add functions to the SparkContext & RDDs.
import com.datastax.spark.connector._


object BasicQueryCassandra {
    def main(args: Array[String]) {
      val sparkMaster = args(0)
      val cassandraHost = args(1)
      val conf = new SparkConf(true)
        .set("spark.cassandra.connection.host", cassandraHost)
      val sc = new SparkContext(sparkMaster, "BasicQueryCassandra", conf)
      // entire table as an RDD
      // assumes your table test was created as CREATE TABLE test.kv(key text PRIMARY KEY, value int);
      val data = sc.cassandraTable("test" , "kv")
      // print some basic stats
      println("stats "+data.map(row => row.getInt("value")).stats())
      val rdd = sc.parallelize(List(("moremagic", 1)))
      rdd.saveToCassandra("test" , "kv", SomeColumns("key", "value"))
      // save from a case class
      val otherRdd = sc.parallelize(List(KeyValue("magic", 0)))
      otherRdd.saveToCassandra("test", "kv")
    }
}

case class KeyValue(key: String, value: Integer)
