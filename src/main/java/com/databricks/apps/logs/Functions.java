package com.databricks.apps.logs;

import com.google.common.base.Optional;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import scala.Tuple4;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public class Functions {
  public static final Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;

  public static final class ValueComparator<K, V>
      implements Comparator<Tuple2<K, V>>, Serializable {
    private Comparator<V> comparator;

    public ValueComparator(Comparator<V> comparator) {
      this.comparator = comparator;
    }

    @Override
    public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
      return comparator.compare(o1._2(), o2._2());
    }
  }

  public static final Function2<List<Long>, Optional<Long>, Optional<Long>>
      COMPUTE_RUNNING_SUM = (nums, current) -> {
    long sum = current.or(0L);
    for (long i : nums) {
      sum += i;
    }
    return Optional.of(sum);
  };

  public static final @Nullable Tuple4<Long, Long, Long, Long> contentSizeStats(
      JavaRDD<ApacheAccessLog> accessLogRDD) {
    JavaRDD<Long> contentSizes =
        accessLogRDD.map(ApacheAccessLog::getContentSize).cache();
    long count = contentSizes.count();
    if (count == 0) {
      return null;
    }
    return new Tuple4<>(count,
        contentSizes.reduce(SUM_REDUCER),
        contentSizes.min(Comparator.naturalOrder()),
        contentSizes.max(Comparator.naturalOrder()));
  }

  public static final JavaPairRDD<Integer, Long> responseCodeCount(
      JavaRDD<ApacheAccessLog> accessLogRDD) {
    return accessLogRDD
        .mapToPair(s -> new Tuple2<>(s.getResponseCode(), 1L))
        .reduceByKey(SUM_REDUCER);
  }

  public static final JavaPairRDD<String, Long> ipAddressCount(
      JavaRDD<ApacheAccessLog> accessLogRDD) {
    return accessLogRDD
        .mapToPair(log -> new Tuple2<>(log.getIpAddress(), 1L))
        .reduceByKey(SUM_REDUCER);
  }

  public static final JavaRDD<String> filterIPAddress(
      JavaPairRDD<String, Long> ipAddressCount) {
    return ipAddressCount
        .filter(tuple -> tuple._2() > 10)
        .map(Tuple2::_1);
  }

  public static final JavaPairRDD<String, Long> endpointCount(
      JavaRDD<ApacheAccessLog> accessLogRDD) {
    return accessLogRDD
        .mapToPair(log -> new Tuple2<>(log.getEndpoint(), 1L))
        .reduceByKey(SUM_REDUCER);
  }
}
