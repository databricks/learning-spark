from pyspark import SparkContext
import csv
import sys
import StringIO


def loadRecord(line):
    """Parse a CSV line"""
    input = StringIO.StringIO(line)
    reader = csv.DictReader(input, fieldnames=["name", "favouriteAnimal"])
    return reader.next()


def loadRecords(fileNameContents):
    """Load all the records in a given file"""
    input = StringIO.StringIO(fileNameContents[1])
    reader = csv.DictReader(input, fieldnames=["name", "favouriteAnimal"])
    return reader


def writeRecords(records):
    """Write out CSV lines"""
    output = StringIO.StringIO()
    writer = csv.DictWriter(output, fieldnames=["name", "favouriteAnimal"])
    for record in records:
        writer.writerow(record)
    return [output.getvalue()]

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print "Error usage: LoadCsv [sparkmaster] [inputfile] [outputfile]"
        sys.exit(-1)
    master = sys.argv[1]
    inputFile = sys.argv[2]
    outputFile = sys.argv[3]
    sc = SparkContext(master, "LoadCsv")
    # Try the record-per-line-input
    input = sc.textFile(inputFile)
    data = input.map(loadRecord)
    pandaLovers = data.filter(lambda x: x['favouriteAnimal'] == "panda")
    pandaLovers.mapPartitions(writeRecords).saveAsTextFile(outputFile)
    # Try the more whole file input
    fullFileData = sc.wholeTextFiles(inputFile).flatMap(loadRecords)
    fullFilePandaLovers = fullFileData.filter(
        lambda x: x['favouriteAnimal'] == "panda")
    fullFilePandaLovers.mapPartitions(
        writeRecords).saveAsTextFile(outputFile + "fullfile")
    sc.stop()
    print "Done!"
