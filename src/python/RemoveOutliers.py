"""
>>> from pyspark.context import SparkContext
>>> sc = SparkContext('local', 'test')
>>> b = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1000])
>>> sorted(removeOutliers(b).collect()
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
"""

import sys
import math

from pyspark import SparkContext


def removeOutliers(nums):
    """Remove the outliers"""
    stats = nums.stats()
    stddev = math.sqrt(stats.variance())
    return nums.filter(lambda x: math.fabs(x - stats.mean()) < 3 * stddev)

if __name__ == "__main__":
    master = "local"
    if len(sys.argv) == 2:
        master = sys.argv[1]
    sc = SparkContext(master, "Sum")
    nums = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1000])
    output = sorted(removeOutliers(nums).collect())
    for num in output:
        print "%i " % (num)
