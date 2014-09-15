# Finds the names of people who like pandas from a parquet file
# consisting of name & favouriteAnimal.
# For input you can use the result of MakeParquetFile
from pyspark import SparkContext
from pyspark.sql import SQLContext
import json
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Error usage: QueryParquetFile [sparkmaster] [parquetfile]"
        sys.exit(-1)
    master = sys.argv[1]
    parquetFile = sys.argv[2]
    sc = SparkContext(master, "QueryParquetFile")
    sqlCtx = SQLContext(sc)
    # Load some data in from a Parquet file of name & favouriteAnimal
    rows = sqlCtx.parquetFile(parquetFile)
    names = rows.map(lambda row: row.name)
    print "Everyone"
    print names.collect()
    # Find the panda lovers
    tbl = rows.registerAsTable("people")
    pandaFriends = sqlCtx.sql("SELECT name FROM people WHERE "+
                              "favouriteAnimal = \"panda\"")
    print "Panda Friends"
    print pandaFriends.map(lambda row: row.name).collect()
