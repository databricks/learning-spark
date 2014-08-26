Examples for Learning Spark
===============
Examples for the Learning Spark book.

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

Scala examples
===

You can build and run the Scala examples with sbt, just run
sbt/sbt compile package run

Java examples
===

You can build and run the Java examples wih maven, just run
mvn package
mvn exec:java -Dexec.mainClass="com.oreilly.learningsparkexamples.java.[EXAMPLE]"

Python examples
===

From spark just run ./bin/pyspark ./src/python/[example]

Spark Submit
===

You can also create an assembly jar with all of the dependcies for running either the java or scala
versions of the code and run the job with the spark-submit script

./sbt/sbt assembly
cd $SPARK_HOME; ./bin/spark-submit   --class com.oreilly.learningsparkexamples.[lang].[example] ../learning-spark-examples/target/scala-2.10/learning-spark-examples-assembly-0.0.1.jar