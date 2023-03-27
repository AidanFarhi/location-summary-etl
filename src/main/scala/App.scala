import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME

object App extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {

    val df = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query", "SELECT * FROM household")
      .load()

    df.show()
  }
}
