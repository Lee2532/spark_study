name := "SparkScalaCourse"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-mllib" % "3.0.0",
  "org.apache.spark" %% "spark-streaming" % "3.0.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.0",
  "mysql" % "mysql-connector-java" % "8.0.11",
)
