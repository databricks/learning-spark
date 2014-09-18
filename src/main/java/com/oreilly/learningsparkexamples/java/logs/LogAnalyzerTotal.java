package com.oreilly.learningsparkexamples.java.logs;

import com.google.common.collect.Ordering;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import scala.Tuple4;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class LogAnalyzerTotal implements Serializable {
  // These static variables stores the running content size values.
  private static final AtomicLong runningCount = new AtomicLong(0);
  private static final AtomicLong runningSum = new AtomicLong(0);
  private static final AtomicLong runningMin = new AtomicLong(Long.MAX_VALUE);
  private static final AtomicLong runningMax = new AtomicLong(Long.MIN_VALUE);
  private static List<Tuple2<Integer, Long>> currentResponseCodeCounts = null;
  private static List<String> currentIPAddresses = null;
  private static List<Tuple2<String, Long>> currentTopEndpoints = null;

  public void processAccessLogs(JavaDStream<ApacheAccessLog> accessLogsDStream) {
    // Calculate statistics based on the content size, and update the static variables to track this.
    accessLogsDStream.foreachRDD(new Function<JavaRDD<ApacheAccessLog>, Void>() {
        public Void call(JavaRDD<ApacheAccessLog> accessLogs) {
          Tuple4<Long, Long, Long, Long> stats =
              Functions.contentSizeStats(accessLogs);
          if (stats != null) {
            runningCount.getAndAdd(stats._1());
            runningSum.getAndAdd(stats._2());
            runningMin.set(Math.min(runningMin.get(), stats._3()));
            runningMax.set(Math.max(runningMax.get(), stats._4()));
          }
          return null;
        }}
      );

    // A DStream of Resonse Code Counts;
    JavaPairDStream<Integer, Long> responseCodeCountDStream =
      Functions.responseCodeCount(accessLogsDStream)
      .updateStateByKey(new Functions.ComputeRunningSum());
    responseCodeCountDStream.foreachRDD(new Function<JavaPairRDD<Integer, Long>, Void>() {
        public Void call(JavaPairRDD<Integer, Long> rdd) {
          currentResponseCodeCounts = rdd.take(100);
          return null;
        }});

    // A DStream of ipAddressCounts.
    JavaPairDStream<String, Long> ipAddressesRawDStream = accessLogsDStream.transform(
      new Function<JavaRDD<ApacheAccessLog>, JavaPairRDD<String, Long>>(){
      public JavaPairRDD<String, Long> call(JavaRDD<ApacheAccessLog> rdd) {
        return LongFunctions.ipAddressCount(rdd);
      }})
      .updateStateByKey(new Functions.ComputeRunningSum());
    // All ips more than 10
    JavaDStream<String> ipAddressDStream = ipAddressesRawDStream.transform(
      new Function<JavaPairRDD<String, Long>, JavaRDD<String>>() {
        public JavaRDD<String> call(JavaPairRDD<String, Long> rdd) {
          Functions.filterIPAddress(rdd);
        }
      });

    ipAddressDStream.foreachRDD(new Function<JavaRDD<String>, Void>() {
        public Void call(JavaRDD<String> rdd) {
          List<String> currentIPAddresses = rdd.take(100);
          return null;
        }});

    // A DStream of endpoint to count.
    JavaPairDStream<String, Long> endpointCountsDStream = accessLogsDStream.transformToPair(
      new Function<JavaRDD<ApacheAccessLog>, JavaPairRDD<String, Long>>() {
        public JavaPairRDD<String, Long> call(JavaRDD<ApacheAccessLog> rdd) {
          return Functions.endpointCount(rdd);
        }
      })
      .updateStateByKey(new Functions.ComputeRunningSum());

    Object ordering = Ordering.natural();
    final Comparator<Long> cmp = (Comparator<Long>)ordering;

    endpointCountsDStream.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
        public Void call(JavaPairRDD<String, Long> rdd) {
      currentTopEndpoints = rdd.takeOrdered(
        10,
        new Functions.ValueComparator<String, Long>(cmp));
      return null;
        }});
  }

  public LogStatistics getLogStatistics() {
    if (runningCount.get() == 0) {
      return LogStatistics.EMPTY_LOG_STATISTICS;
    }

    return new LogStatistics(new Tuple4<>(runningCount.get(), runningSum.get(),
        runningMin.get(), runningMax.get()),
        currentResponseCodeCounts, currentIPAddresses, currentTopEndpoints);
  }
}
