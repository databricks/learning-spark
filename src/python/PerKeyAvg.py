"""
>>> from pyspark.context import SparkContext
>>> sc = SparkContext('local', 'test')
>>> input = [("coffee", 1), ("pandas", 2), ("coffee", 3), ("very", 4)]
>>> b = sc.parallelize(input)
>>> perKeyAvg(b)

"""

import sys

from pyspark import SparkContext


def perKeyAvg(nums):
    """Compute the avg"""
    sumCount = nums.combineByKey((lambda x: (x, 1)),
                                 (lambda x, y: (x[0] + y, x[1] + 1)),
                                 (lambda x, y: (x[0] + y[0], x[1] + y[1])))
    return sumCount.collectAsMap()

if __name__ == "__main__":
    master = "local"
    if len(sys.argv) == 2:
        master = sys.argv[1]
    sc = SparkContext(master, "Sum")
    nums = sc.parallelize(
        [("coffee", 1), ("pandas", 2), ("coffee", 3), ("very", 4)])
    avg = perKeyAvg(nums)
    print avg
