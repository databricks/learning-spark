package com.oreilly.learningsparkexamples.java.logs;

import com.google.common.base.Optional;
import com.google.common.collect.Ordering;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple4;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public class Functions {
  public static final class LongSumReducer implements Function2<Long, Long, Long> {
    @Override
    public Long call(Long a, Long b) {
      return  a + b;
    }
  };

  public static final class SumReducer implements Function2<Double, Double, Double> {
    @Override
    public Double call(Double a, Double b) {
      return  a + b;
    }
  };


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

  public static final class ComputeRunningSum implements Function2<List<Long>, Optional<Long>, Optional<Long>> {
    @Override
    public Optional<Long> call(List<Long> nums, Optional<Long> current) {
      long sum = current.or(0L);
      for (long i : nums) {
        sum += i;
      }
      return Optional.of(sum);
    }
  };

  public static final class GetContentSize implements DoubleFunction<ApacheAccessLog> {
    @Override
    public double call(ApacheAccessLog log) {
      return new Long(log.getContentSize()).doubleValue();
    }
  }

  public static final @Nullable Tuple4<Long, Long, Long, Long> contentSizeStats(
      JavaRDD<ApacheAccessLog> accessLogRDD) {
    JavaDoubleRDD contentSizes =
      accessLogRDD.mapToDouble(new GetContentSize()).cache();
    long count = contentSizes.count();
    if (count == 0) {
      return null;
    }
    Object ordering = Ordering.natural();
    final Comparator<Double> cmp = (Comparator<Double>)ordering;
    
    return new Tuple4<>(count,
                        contentSizes.reduce(new SumReducer()).longValue(),
                        contentSizes.min(cmp).longValue(),
                        contentSizes.max(cmp).longValue());
  }

  public static final class ResponseCodeTuple implements PairFunction<ApacheAccessLog, Integer, Long> {
    @Override
    public Tuple2<Integer, Long> call(ApacheAccessLog log) {
      return new Tuple2<>(log.getResponseCode(), 1L);
    }
  }

  public static final JavaPairRDD<Integer, Long> responseCodeCount(
      JavaRDD<ApacheAccessLog> accessLogRDD) {
    return accessLogRDD
      .mapToPair(new ResponseCodeTuple())
      .reduceByKey(new LongSumReducer());
  }

  public static final class IpTuple implements PairFunction<ApacheAccessLog, String, Long> {
    @Override
    public Tuple2<String, Long> call(ApacheAccessLog log) {
      return new Tuple2<>(log.getIpAddress(), 1L);
    }
  }

  public static final class IpContentTuple implements PairFunction<ApacheAccessLog, String, Long> {
    @Override
    public Tuple2<String, Long> call(ApacheAccessLog log) {
      return new Tuple2<>(log.getIpAddress(), log.getContentSize());
    }
  }


  public static final class EndPointTuple implements PairFunction<ApacheAccessLog, String, Long> {
    @Override
    public Tuple2<String, Long> call(ApacheAccessLog log) {
      return new Tuple2<>(log.getEndpoint(), 1L);
    }
  }


  public static final class IpCountGreaterThan10 implements Function<Tuple2<String, Long>, Boolean> {
    @Override
    public Boolean call(Tuple2<String, Long> e) {
      return e._2() > 10;
    }
  }

  public static final class ParseFromLogLine implements Function<String, ApacheAccessLog> {
    @Override
    public ApacheAccessLog call(String line) {
      return ApacheAccessLog.parseFromLogLine(line);
    }
  }
  public static final JavaPairRDD<String, Long> ipAddressCount(
      JavaRDD<ApacheAccessLog> accessLogRDD) {
    return accessLogRDD
      .mapToPair(new IpTuple())
      .reduceByKey(new LongSumReducer());
  }

  public static final JavaRDD<String> filterIPAddress(
      JavaPairRDD<String, Long> ipAddressCount) {
    return ipAddressCount
      .filter(new IpCountGreaterThan10())
      .keys();
  }

  public static final JavaPairRDD<String, Long> endpointCount(
      JavaRDD<ApacheAccessLog> accessLogRDD) {
    return accessLogRDD
      .mapToPair(new EndPointTuple())
      .reduceByKey(new LongSumReducer());
  }
}
