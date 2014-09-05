"""
>>> from pyspark.context import SparkContext
>>> sc = SparkContext('local', 'test')
>>> input = ["coffee", "i really like coffee", "coffee > magic"]
>>> b = sc.parallelize(input)
>>> sorted(basicKeyValueMapFilter(b).collect())
[4, 9]
"""

import sys

from pyspark import SparkContext


def basicKeyValueMapFilter(input):
    """Construct a key/value RDD and then filter on the value"""
    return input.map(lambda x: (x.split(" ")[0], x)).filter(
        lambda x: len(x[1]) < 20)

if __name__ == "__main__":
    master = "local"
    if len(sys.argv) == 2:
        master = sys.argv[1]
    sc = SparkContext(master, "BasicFilterMap")
    input = sc.parallelize(
        ["coffee", "i really like coffee", "coffee > magic", "panda < coffee"])
    output = sorted(basicKeyValueMapFilter(input).collect())
    for elem in output:
        print elem
