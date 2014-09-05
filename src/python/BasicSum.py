"""
>>> from pyspark.context import SparkContext
>>> sc = SparkContext('local', 'test')
>>> b = sc.parallelize([1, 2, 3, 4])
>>> basicSum(b)
10
"""

import sys

from pyspark import SparkContext


def basicSum(nums):
    """Sum the numbers"""
    return nums.fold(0, (lambda x, y: x + y))

if __name__ == "__main__":
    master = "local"
    if len(sys.argv) == 2:
        master = sys.argv[1]
    sc = SparkContext(master, "Sum")
    nums = sc.parallelize([1, 2, 3, 4])
    output = basicSum(nums)
    print output
