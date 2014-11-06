# A simple demo for working with SparkSQL and Tweets
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
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
    sc.stop()
