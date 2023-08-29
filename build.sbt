name := "IndexedDF"

version := "1.0"

ThisBuild / scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-yarn" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided"
//  "org.apache.spark" %% "spark-core" % sparkVersion,
//  "org.apache.spark" %% "spark-sql" % sparkVersion,
//  "org.apache.spark" %% "spark-yarn" % sparkVersion,
//  "org.apache.spark" %% "spark-hive" % sparkVersion,
//  "org.apache.spark" %% "spark-catalyst" % sparkVersion
)

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-api" % "2.1.2"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.5"
