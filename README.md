Examples for Learning Spark
===============
Examples for the Learning Spark book. 

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