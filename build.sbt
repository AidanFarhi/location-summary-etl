scalaVersion := "2.13.10"
val sparkVersion = "3.3.2"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyJarName in assembly := "location-summary-etl-LATEST.jar"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "net.snowflake" %% "spark-snowflake" % "2.11.2-spark_3.3",
  "net.snowflake" % "snowflake-jdbc" % "3.13.29"
)
