from pyspark import SparkContext
import json
import sys

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print "Error usage: LoadJson [sparkmaster] [inputfile] [outputfile]"
        sys.exit(-1)
    master = sys.argv[1]
    inputFile = sys.argv[2]
    outputFile = sys.argv[3]
    sc = SparkContext(master, "LoadJson")
    input = sc.textFile(inputFile)
    data = input.map(lambda x: json.loads(x))
    data.filter(lambda x: 'lovesPandas' in x and x['lovesPandas']).map(
        lambda x: json.dumps(x)).saveAsTextFile(outputFile)
    sc.stop()
    print "Done!"
