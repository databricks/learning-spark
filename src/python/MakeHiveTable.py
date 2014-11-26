# Createas a hive table and loads an input file into it
# For input you can use examples/src/main/resources/kv1.txt from the spark
# distribution
from pyspark import SparkContext
from pyspark.sql import HiveContext
import json
import sys

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print "Error usage: LoadHive [sparkmaster] [inputFile] [inputtable]"
        sys.exit(-1)
    master = sys.argv[1]
    inputFile = sys.argv[2]
    inputTable = sys.argv[3]
    sc = SparkContext(master, "LoadHive")
    hiveCtx = HiveContext(sc)
    # Load some data into hive
    hiveCtx.sql(
        "CREATE TABLE IF NOT EXISTS " +
        inputTable +
        " (key INT, value STRING)")
    hiveCtx.sql(
        "LOAD DATA LOCAL INPATH '" + inputFile + "' INTO TABLE " + inputTable)
