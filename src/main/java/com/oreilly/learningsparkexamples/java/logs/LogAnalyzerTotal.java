package com.databricks.apps.logs;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
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
    accessLogsDStream.foreachRDD(accessLogs -> {
          Tuple4<Long, Long, Long, Long> stats =
              Functions.contentSizeStats(accessLogs);
          if (stats != null) {
            runningCount.getAndAdd(stats._1());
            runningSum.getAndAdd(stats._2());
            runningMin.set(Math.min(runningMin.get(), stats._3()));
            runningMax.set(Math.max(runningMax.get(), stats._4()));
          }
          return null;
        }
    );

    // A DStream of Resonse Code Counts;
    JavaPairDStream<Integer, Long> responseCodeCountDStream = accessLogsDStream
        .transformToPair(Functions::responseCodeCount)
        .updateStateByKey(Functions.COMPUTE_RUNNING_SUM);
    responseCodeCountDStream.foreachRDD(rdd -> {
      currentResponseCodeCounts = rdd.take(100);
      return null;
    });

    // A DStream of ipAddressCounts.
    JavaDStream<String> ipAddressesDStream = accessLogsDStream
        .transformToPair(Functions::ipAddressCount)
        .updateStateByKey(Functions.COMPUTE_RUNNING_SUM)
        .transform(Functions::filterIPAddress);
    ipAddressesDStream.foreachRDD(rdd -> {
      currentIPAddresses = rdd.take(100);
      return null;
    });

    // A DStream of endpoint to count.
    JavaPairDStream<String, Long> endpointCountsDStream = accessLogsDStream
        .transformToPair(Functions::endpointCount)
        .updateStateByKey(Functions.COMPUTE_RUNNING_SUM);
    endpointCountsDStream.foreachRDD(rdd -> {
      currentTopEndpoints = rdd.takeOrdered(10,
          new Functions.ValueComparator<>(Comparator.<Long>naturalOrder()));
      return null;
    });
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
