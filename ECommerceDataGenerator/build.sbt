ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "ECommerceDataGenerator"
  )


libraryDependencies ++= Seq(
  // Spark Core and SQL
  "org.apache.spark" %% "spark-core" % "4.0.0",
  "org.apache.spark" %% "spark-sql"  % "4.0.0",

  // Hive Metastore
  "org.apache.spark" %% "spark-hive" % "4.0.0",

  // Hadoop AWS and Common
  "org.apache.hadoop" % "hadoop-aws"    % "3.3.6",
  "org.apache.hadoop" % "hadoop-common" % "3.3.6",

  // AWS Java SDK Bundle
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.797"
)