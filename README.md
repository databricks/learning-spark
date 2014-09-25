---
title: Examples for Learning Spark
customjs:
 - http://www.tqlkg.com/widget-54245b33e4b050ed3e73caf4-7645222?target=_top&mouseover=Y
---
![buildstatus](https://travis-ci.org/holdenk/learning-spark-examples.svg?branch=master)(https://travis-ci.org/holdenk/learning-spark-examples)
Examples for Learning Spark
===============
Examples for the Learning Spark book. These examples require a number of libraries and as such have long build files. We have also added a stand alone example with minimal dependcies and a small build file
in the mini-complete-example directory.

Requirements
==
* JDK 1.6 or higher
* Scala 2.10.3
- scala-lang.org
* Spark 1.0 sanp shot
- You can checkout spark from https://github.com/apache/spark and then run "sbt/sbt publish-local"
* Protobuf compiler
- On debian you can install with sudo apt-get install protobuf-compiler
* R & the CRAN package Imap are required for the ChapterSixExample
* The Python examples require urllib3

Python examples
===

From spark just run ./bin/pyspark ./src/python/[example]

Spark Submit
===

You can also create an assembly jar with all of the dependcies for running either the java or scala
versions of the code and run the job with the spark-submit script

./sbt/sbt assembly OR mvn package
cd $SPARK_HOME; ./bin/spark-submit   --class com.oreilly.learningsparkexamples.[lang].[example] ../learning-spark-examples/target/scala-2.10/learning-spark-examples-assembly-0.0.1.jar

<!-- Custom JavaScript files set in YAML front matter -->
{% for js in page.customjs %}
<script async type="text/javascript" src="{{ js }}"></script>
{% endfor %}