scalaVersion := "2.12.10"
val sparkVersion = "3.3.2"

assemblyJarName in assembly := "location-summary-etl-LATEST.jar"

assemblyMergeStrategy in assembly := {
  case x if x.startsWith("mozilla") => MergeStrategy.filterDistinctLines
  case x => val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "net.snowflake" %% "spark-snowflake" % "2.11.2-spark_3.3",
  "net.snowflake" % "snowflake-jdbc" % "3.13.29"
)
