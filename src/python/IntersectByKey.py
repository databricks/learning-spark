"""
>>> from pyspark.context import SparkContext
>>> sc = SparkContext('local', 'test')
>>> input = [("coffee", 1), ("pandas", 2), ("coffee", 3), ("very", 4)]
>>> rdd1 = sc.parallelize(input)
>>> rdd2 = sc.parallelize([("pandas", 20)])
>>> intserectByKey(rdd1, rdd2).collect()
[('pandas', 2), ('pandas', 20)]
"""

import sys
import itertools

from pyspark import SparkContext


def combineIfBothPresent(itrs):
    """Return an iterable of the elements from
    both itr1 and itr2 if there are elements in both itr1 and itr2 otherwise
    return an empty itrable"""
    iter1 = itrs[0].__iter__()
    iter2 = itrs[1].__iter__()
    try:
        e1 = iter1.next()
        e2 = iter2.next()
        return itertools.chain([e1], [e2], iter1, iter2)
    except StopIteration:
        return []


def intersectByKey(rdd1, rdd2):
    """Intersect two RDDs by key"""
    return rdd1.cogroup(rdd2).flatMapValues(combineIfBothPresent)

if __name__ == "__main__":
    master = "local"
    if len(sys.argv) == 2:
        master = sys.argv[1]
    sc = SparkContext(master, "IntersectByKey")
    rdd1 = sc.parallelize(
        [("coffee", 1), ("pandas", 2), ("coffee", 3), ("very", 4)])
    rdd2 = sc.parallelize([("pandas", 20), ("pandas", 21)])
    print intersectByKey(rdd1, rdd2).collect()
