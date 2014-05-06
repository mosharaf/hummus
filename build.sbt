name := "hummus"

version := "0.1.0"

scalaVersion := "2.9.3"

libraryDependencies += "org.apache.spark" % "spark-core_2.9.3" % "0.8.1-incubating"

excludeFilter in unmanagedSources := "scripts.scala"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

ivyXML := <dependency org="org.eclipse.jetty.orbit" name="javax.servlet" rev="2.5.0.v201103041518"> <artifact name="javax.servlet" type="orbit" ext="jar"/> </dependency>
