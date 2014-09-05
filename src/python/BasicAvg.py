"""
>>> from pyspark.context import SparkContext
>>> sc = SparkContext('local', 'test')
>>> b = sc.parallelize([1, 2, 3, 4])
>>> basicAvg(b)
2.5
"""

import sys

from pyspark import SparkContext


def basicAvg(nums):
    """Compute the avg"""
    sumCount = nums.map(lambda x: (x, 1)).fold(
        (0, 0), (lambda x, y: (x[0] + y[0], x[1] + y[1])))
    return sumCount[0] / float(sumCount[1])

if __name__ == "__main__":
    master = "local"
    if len(sys.argv) == 2:
        master = sys.argv[1]
    sc = SparkContext(master, "Sum")
    nums = sc.parallelize([1, 2, 3, 4])
    avg = basicAvg(nums)
    print avg
