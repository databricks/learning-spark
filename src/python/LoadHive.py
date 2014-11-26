# A simple hive demo. If you do not have a table to load from look run
# MakeHiveTable.py
from pyspark import SparkContext
from pyspark.sql import HiveContext
import json
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Error usage: LoadHive [sparkmaster] [inputtable]"
        sys.exit(-1)
    master = sys.argv[1]
    inputTable = sys.argv[2]
    sc = SparkContext(master, "LoadHive")
    hiveCtx = HiveContext(sc)
    # Query hive
    input = hiveCtx.sql("FROM " + inputTable + " SELECT key, value")
    print "result of query"
    print input.collect()
    data = input.map(lambda x: x[0] * x[0])
    result = data.collect()
    for element in result:
        print "Got data " + str(element)
    sc.stop()
    print "Done!"
