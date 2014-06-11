# A simple hive demo. Createas a hive table and loads an input file into it then selects the result
# For input you can use examples/src/main/resources/kv1.txt from the spark distribution
from pyspark import SparkContext
from pyspark.sql import HiveContext
import json
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Error usage: LoadHive [sparkmaster] [inputfile]"
        sys.exit(-1)
    master = sys.argv[1]
    inputFile = sys.argv[2]
    sc = SparkContext(master, "LoadHive")
    hiveCtx = HiveContext(sc)
    # Load some data into hive
    hiveCtx.hql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    hiveCtx.hql("LOAD DATA LOCAL INPATH '" + inputFile + "' INTO TABLE src")
    # Query hive
    input = hiveCtx.hql("FROM src SELECT key, value")
    data = input.map(lambda x: x['key'] * x['key'])
    result = data.collect()
    for element in result:
        print "Got data " + str(element)
    sc.stop()
    print "Done!"
