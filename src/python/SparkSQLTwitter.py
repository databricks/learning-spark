# A simple demo for working with SparkSQL and Tweets
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
import json
import sys

if __name__ == "__main__":
    inputFile = sys.argv[1]
    conf = SparkConf().setAppName("SparkSQLTwitter")
    sc = SparkContext()
    sqlCtx = SQLContext(sc)
    print "Loading tweets from " + inputFile
    input = sqlCtx.jsonFile(inputFile)
    input.registerTempTable("tweets")
    topTweets = sqlCtx.sql("SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10")
    print topTweets.collect()
    topTweetText = topTweets.map(lambda row : row.text)
    print topTweetText.collect()
    # Make a happy person row
    happyPeopleRDD = sc.parallelize([Row(name="holden", favouriteBeverage="coffee")])
    happyPeopleSchemaRDD = sqlCtx.inferSchema(happyPeopleRDD)
    happyPeopleSchemaRDD.registerTempTable("happy_people")
    # Make a UDF to tell us how long some text is
    sqlCtx.registerFunction("strLenPython", lambda x: len(x), IntegerType())
    lengthSchemaRDD = sqlCtx.sql("SELECT strLenPython('text') FROM tweets LIMIT 10")
    print lengthSchemaRDD.collect()
    sc.stop()
