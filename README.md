# Hummus

Traceroute Analysis Toolkit based on Apache Spark

## Making Hummus
Hummus is built on `Scala 2.9.3` and `Apache Spark 0.8.1-incubating`. 

To build Hummus, run:

    sbt package

You must also have sbt installed in your system. Instructions here: <http://www.scala-sbt.org/release/docs/Getting-Started/Setup.html>

## Running Hummus
Once you have compiled Hummus, the easiest way to run any of the analysis tools, for example `AS_Analysis`, is 

	<Path_to_Spark>/run-example arkiplane.AS_Analysis <Spark_Master_URL> <Dataset_Name>

## Adhoc Analysis
You can use the `spark-shell` for adhoc, exploratory analysis. 

	<Path_to_Spark>/spark-shell

More information on Spark can be found at <http://spark.apache.org/documentation.html>.
