// build.sbt

val scala212 = "2.12.18"
val sparkVersion = "3.5.1"

lazy val root = (project in file("."))
  .settings(
    name := "SparkDataChallenge", // Or your project name like "sparkwithscalaproject"
    version := "0.1.0",
    scalaVersion := scala212,

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.2.18" % Test,

  // AWS Dependencies - Align hadoop-aws with Spark's Hadoop version
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4", 
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.367" 
    )
  )