scalaVersion := "2.11.12"
val sparkVersion = "2.3.0"

name := "scurvy"
organization := "com.orangutanalytics"
version := "0.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
