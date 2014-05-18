from pyspark import SparkContext
import json
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Error usage: LoadJson [sparkmaster] [file]"
        sys.exit(-1)
    sc = SparkContext(sys.argv[1], "LoadJson")
    input = sc.textFile(sys.argv[2])
    data = input.map(lambda x: json.loads(x))
    print data.collect()

