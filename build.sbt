scalaVersion := "2.12.10"
val sparkVersion = "3.3.2"

assemblyMergeStrategy in assembly := {
  // case PathList("META-INF", xs @ _*) => MergeStrategy.rename
  case x => MergeStrategy.rename
}

assemblyJarName in assembly := "location-summary-etl-LATEST.jar"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "net.snowflake" %% "spark-snowflake" % "2.11.2-spark_3.3",
  "net.snowflake" % "snowflake-jdbc" % "3.13.29"
)
