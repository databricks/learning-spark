# A simple demo for working with SparkSQL and Tweets
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import json
import sys

if __name__ == "__main__":
    conf = SparkConf().setAppName("SparkSQLTwitter")
    sc = SparkContext()
    sqlCtx = SQLContext(sc)
    sc.stop()
