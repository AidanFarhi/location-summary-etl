import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master(sys.env("SPARK_MASTER_URL"))
      .appName("location-summary-etl")
      .getOrCreate()
  }

  lazy val sfOptions: Map[String, String] = {
    List(
      "sfURL" -> sys.env("SNOWFLAKE_URL"),
      "sfUser" -> sys.env("SNOWFLAKE_USER"),
      "sfPassword" -> sys.env("SNOWFLAKE_PASSWORD"),
      "sfDatabase" -> sys.env("SNOWFLAKE_DATABASE"),
      "sfSchema" -> sys.env("SNOWFLAKE_SCHEMA"),
      "sfWarehouse" -> sys.env("SNOWFLAKE_WAREHOUSE"),
    ).toMap
  }
}
