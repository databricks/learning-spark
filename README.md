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