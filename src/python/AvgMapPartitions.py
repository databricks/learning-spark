"""
>>> from pyspark.context import SparkContext
>>> sc = SparkContext('local', 'test')
>>> b = sc.parallelize([1, 2, 3, 4])
>>> avg(b)
2.5
"""

import sys

from pyspark import SparkContext


def partitionCtr(nums):
    """Compute sumCounter for partition"""
    sumCount = [0, 0]
    for num in nums:
        sumCount[0] += num
        sumCount[1] += 1
    return [sumCount]


def combineCtrs(c1, c2):
    return (c1[0] + c2[0], c1[1] + c2[1])


def basicAvg(nums):
    """Compute the avg"""
    sumCount = nums.mapPartitions(partitionCtr).reduce(combineCtrs)
    return sumCount[0] / float(sumCount[1])

if __name__ == "__main__":
    cluster = "local"
    if len(sys.argv) == 2:
        cluster = sys.argv[1]
    sc = SparkContext(cluster, "Sum")
    nums = sc.parallelize([1, 2, 3, 4])
    avg = basicAvg(nums)
    print avg
