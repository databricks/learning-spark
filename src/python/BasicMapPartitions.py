"""
>>> from pyspark.context import SparkContext
>>> sc = SparkContext('local', 'test')
>>> b = sc.parallelize(["KK6JKQ", "Ve3UoW", "kk6jlk", "W6BB"])
>>> fetchCallSigns(b).size()
4
"""

import sys
import urllib3

from pyspark import SparkContext


def processCallSigns(signs):
    """Process call signs"""
    http = urllib3.PoolManager()
    requests = map(
        lambda x: http.request('GET', "http://qrzcq.com/call/" + x), signs)
    return map(lambda x: x.data, requests)


def fetchCallSigns(input):
    """Fetch call signs"""
    return input.mapPartitions(lambda callSigns: processCallSigns(callSigns))

if __name__ == "__main__":
    master = "local"
    if len(sys.argv) == 2:
        master = sys.argv[1]
    sc = SparkContext(master, "BasicMapPartitions")
    input = sc.parallelize(["KK6JKQ", "Ve3UoW", "kk6jlk", "W6BB"])
    output = sorted(fetchCallSigns(input).collect())
    for str in output:
        print "%s " % (str)
