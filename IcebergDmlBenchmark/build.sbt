ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "IcebergDmlBenchmark"
  )
val sparkVersion = "4.0.0"
//val sparkVersion = "3.5.1"
libraryDependencies ++= Seq(
  // Spark Core and SQL
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,

  // Hive Metastore
  "org.apache.spark" %% "spark-hive" % sparkVersion,

  // Iceberg Spark Runtime
  "org.apache.iceberg" %% "iceberg-spark-runtime-4.0" % "1.10.1",
  //  "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % "1.5.2",

  // Hadoop AWS and Common
  //  "org.apache.hadoop" % "hadoop-aws"    % "3.3.6",
  //  "org.apache.hadoop" % "hadoop-common" % "3.3.6",
  "org.apache.spark" %% "spark-hadoop-cloud" % sparkVersion,

  // AWS Java SDK Bundle
  // REQUIRED for Hadoop 3.4.x (Spark 4.0.0) - AWS SDK v2 bundle
  "software.amazon.awssdk" % "bundle" % "2.47.0"
  //  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.797"
)