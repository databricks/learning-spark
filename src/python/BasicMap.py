"""
>>> from pyspark.context import SparkContext
>>> sc = SparkContext('local', 'test')
>>> b = sc.parallelize([1, 2, 3, 4])
>>> sorted(basicSquare(b).collect())
[1, 4, 9, 16]
"""

import sys

from pyspark import SparkContext


def basicSquare(nums):
    """Square the numbers"""
    return nums.map(lambda x: x * x)

if __name__ == "__main__":
    master = "local"
    if len(sys.argv) == 2:
        master = sys.argv[1]
    sc = SparkContext(master, "BasicMap")
    nums = sc.parallelize([1, 2, 3, 4])
    output = sorted(basicSquare(nums).collect())
    for num in output:
        print "%i " % (num)
