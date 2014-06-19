/**
 * Illustrates loading data over JDBC
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import org.apache.spark.rdd.JdbcRDD
import java.sql.{DriverManager, ResultSet}

object LoadSimpleJdbc {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage: [sparkmaster]")
      exit(1)
    }
    val master = args(0)
    val sc = new SparkContext(master, "LoadSimpleJdbc", System.getenv("SPARK_HOME"))
    val data = new JdbcRDD(sc,
      createConnection, "SELECT * FROM panda WHERE ? <= id AND ID <= ?",
      lowerBound = 1, upperBound = 3, numPartitions = 2, mapRow = extractValues)
    println(data.collect().toList)
  }

  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    DriverManager.getConnection("jdbc:mysql://localhost/test?user=holden");
  }

  def extractValues(r: ResultSet) = {
    (r.getInt(1), r.getString(2))
  }
}
