name := "qaFrameWork"

version := "1.0.11"

scalaVersion := "2.11.12"

val spark_kinesis_version = "2.2.0"

val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
)
