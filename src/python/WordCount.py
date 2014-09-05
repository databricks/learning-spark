import sys

from pyspark import SparkContext

if __name__ == "__main__":
    master = "local"
    if len(sys.argv) == 2:
        master = sys.argv[1]
    sc = SparkContext(master, "WordCount")
    lines = sc.parallelize(["pandas", "i like pandas"])
    result = lines.flatMap(lambda x: x.split(" ")).countByValue()
    for key, value in result.iteritems():
        print "%s %i" % (key, value)
