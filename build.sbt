name := "roo-spark-examples"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.3",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.3",
  "org.apache.spark" % "spark-streaming_2.11" % "2.1.3",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.1.3",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.1.3"
)

