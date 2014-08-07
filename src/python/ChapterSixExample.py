"""Contains the Chapter 6 Example illustrating accumulators, broadcast variables, numeric operations, and pipe."""
import sys
import re

from pyspark import SparkContext

inputFile = sys.argv[1]
outputDir = sys.argv[2]

sc = SparkContext(appName="ChapterSixExample")
file = sc.textFile(inputFile)

# Create Accumulator[Int] initialized to 0
blankLines = sc.accumulator(0)
dataLines = sc.accumulator(0)

def extractCallSigns(line):
    global blankLines, dataLines # Access the counters
    if (line == ""):
        blankLines += 1
    return line.split(" ")

callSigns = file.flatMap(extractCallSigns)
callSigns.saveAsTextFile(outputDir + "/callsigns")
print "Blank lines %d" % blankLines.value

# Create Accumulators for validating call signs
validSignCount = sc.accumulator(0)
invalidSignCount = sc.accumulator(0)

def validateSign(sign):
    global validSignCount, invalidSignCount
    if re.match(r"\A\d?[a-zA-Z]{1,2}\d{1,4}[a-zA-Z]{1,3}\Z", sign):
        validSignCount += 1
        return True
    else:
        invalidSignCount += 1
        return False


validSigns = callSigns.filter(validateSign)
# Force evaluation so the counters are populated
validSigns.count()
if invalidSignCount.value < 0.1 * validSignCount.value:
    validSigns.saveAsTextFile(outputDir + "/validsigns")
else:
    print "Too many errors %d in %d" % (invalidSignCount.value, validSignCount.value)
