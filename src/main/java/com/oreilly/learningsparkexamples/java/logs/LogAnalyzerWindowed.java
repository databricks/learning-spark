package com.oreilly.learningsparkexamples.java.logs;

import com.google.common.collect.Ordering;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import scala.Tuple4;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public class LogAnalyzerWindowed implements Serializable {
  private LogStatistics logStatistics;

  public void processAccessLogs(JavaDStream<ApacheAccessLog> accessLogsDStream) {
    JavaDStream<ApacheAccessLog> windowDStream = accessLogsDStream.window(
        Flags.getInstance().getWindowLength(),
        Flags.getInstance().getSlideInterval());
    windowDStream.foreachRDD(new Function<JavaRDD<ApacheAccessLog>, Void>() {
        public Void call(JavaRDD<ApacheAccessLog> accessLogs) {
      Tuple4<Long, Long, Long, Long> contentSizeStats =
          Functions.contentSizeStats(accessLogs);

      List<Tuple2<Integer, Long>> responseCodeToCount =
          Functions.responseCodeCount(accessLogs)
          .take(100);

      JavaPairRDD<String, Long> ipAddressCounts =
          Functions.ipAddressCount(accessLogs);
      List<String> ipAddresses = Functions.filterIPAddress(ipAddressCounts)
          .take(100);

      Object ordering = Ordering.natural();
      Comparator<Long> cmp = (Comparator<Long>)ordering;
      List<Tuple2<String, Long>> topEndpoints =
          Functions.endpointCount(accessLogs)
        .top(10, new Functions.ValueComparator<String, Long>(cmp));

      logStatistics = new LogStatistics(contentSizeStats, responseCodeToCount,
          ipAddresses, topEndpoints);
      return null;
        }});
  }

  public LogStatistics getLogStatistics() {
    return logStatistics;
  }
}
