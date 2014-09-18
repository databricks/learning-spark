package com.oreilly.learningsparkexamples.java.logs;

import scala.Tuple2;
import scala.Tuple4;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogStatistics implements Serializable {
  public final static LogStatistics EMPTY_LOG_STATISTICS =
    new LogStatistics(new Tuple4<>(0L, 0L, 0L, 0L), new ArrayList<Tuple2<Integer, Long>>(),
                      new ArrayList<String>(), new ArrayList<Tuple2<String, Long>>());

  private Tuple4<Long, Long, Long, Long> contentSizeStats;
  private List<Tuple2<Integer, Long>> responseCodeToCount;
  private List<String> ipAddresses;
  private List<Tuple2<String, Long>> topEndpoints;

  public LogStatistics(Tuple4<Long, Long, Long, Long> contentSizeStats,
                       List<Tuple2<Integer, Long>> responseCodeToCount,
                       List<String> ipAddresses,
                       List<Tuple2<String, Long>> topEndpoints) {
    this.contentSizeStats = contentSizeStats;
    this.responseCodeToCount = responseCodeToCount;
    this.ipAddresses = ipAddresses;
    this.topEndpoints = topEndpoints;
  }

  public Tuple4<Long, Long, Long, Long> getContentSizeStats() {
    return contentSizeStats;
  }

  public Map<Integer, Long> getResponseCodeToCount() {
    Map<Integer, Long> responseCodeCount = new HashMap<>();
    for (Tuple2<Integer, Long> tuple: responseCodeToCount) {
      responseCodeCount.put(tuple._1(), tuple._2());
    }
    return responseCodeCount;
  }

  public List<String> getIpAddresses() {
    return ipAddresses;
  }

  public List<Tuple2<String, Long>> getTopEndpoints() {
    return topEndpoints;
  }
}
