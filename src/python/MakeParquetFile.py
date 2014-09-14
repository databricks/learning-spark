# Createas a parquet file and loads an input file into it
# For input you can use files/favourite_animal.csv as the iput
from pyspark import SparkContext
from pyspark.sql import SQLContext
import json
import sys

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print "Error usage: LoadHive [sparkmaster] [inputFile] [parquetfile]"
        sys.exit(-1)
    master = sys.argv[1]
    inputFile = sys.argv[2]
    parquetFile = sys.argv[3]
    sc = SparkContext(master, "MakeParquetFile")
    sqlCtx = SQLContext(sc)
    # Load some data into an RDD
    rdd = sc.textFile(inputFile).map(lambda l: l.split(","))
    namedRdd = rdd.map(lambda r: {"name": r[0], "favouriteAnimal": r[1]})
    schemaNamedRdd = sqlCtx.inferSchema(namedRdd)
    # Save it
    schemaNamedRdd.saveAsParquetFile(parquetFile)
