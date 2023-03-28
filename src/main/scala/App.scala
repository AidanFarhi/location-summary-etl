import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.functions.{avg, col, year, current_date}

object App extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {

    val householdDf = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query",
      """
      SELECT household_id FROM household
      WHERE
        adults = 1 AND
        working_adults = 1 AND
        children = 2
      """
      )
      .load()

    val wageDf = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query",
      """
      WITH max_date_cte AS (SELECT MAX(as_of_date) max_date FROM wage)
      SELECT * FROM wage
      WHERE
        wage_level LIKE '%LIVING%' AND
        as_of_date = (SELECT max_date FROM max_date_cte)
      """
      ).load()

    val locationDf = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query", "SELECT zip_code, state, county FROM location WHERE state = 'DE'")
      .load()

    val listingDf = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query",
      """
      WITH max_date_cte AS (SELECT MAX(snapshot_date) max_date FROM listing)
      SELECT
        zip_code,
        price,
        bathrooms,
        bedrooms,
        square_footage,
        year_built
      FROM listing
      WHERE snapshot_date = (SELECT max_date FROM max_date_cte)
      """
      )
      .load()
      .withColumn("age_in_years", year(current_date()) - col("year_built"))

    // Calculate recommended annual salary
    val recommendedSalaryDf = wageDf.join(householdDf, Seq("household_id"), "inner")
      .withColumn("recommended_annual_salary", col("hourly_wage") * 40 * 52)
    recommendedSalaryDf.show()

    // Calculate AVGs for selected listing columns
    val columnNames = List("price", "bathrooms", "bedrooms", "age_in_years", "square_footage")
    val avgCols = columnNames.map(c => avg(col(c)).alias(s"avg_$c"))
    val listingAvgDf = listingDf.groupBy("zip_code").agg(avgCols.head, avgCols.tail: _*)

    // Join in location df
    val listingAndLocationDf = listingAvgDf.join(locationDf, Seq("zip_code"), "inner")
    listingAndLocationDf.show()

    // Join in recommended salary df
    val listingLocationRecommendedSalaryDf = listingAndLocationDf
      .join(recommendedSalaryDf, Seq("county"), "inner")

    // TODO: COUNTY attribute does not match in LOCATION and WAGE tables
    listingLocationRecommendedSalaryDf.show()
  }
}
