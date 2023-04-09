import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.functions.{avg, col, current_date, desc, max, min, year}

object App extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {

    val dimCrimeRate = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query",
      """
      WITH latest_crime_rates AS (
        SELECT location_id, crime_type, MAX(snapshot_date) AS max_date
        FROM dim_crime_rate
        GROUP BY location_id, crime_type
      )
      SELECT dcm.*
      FROM dim_crime_rate dcm
      JOIN latest_crime_rates lcm
      ON dcm.location_id = lcm.location_id
      AND dcm.crime_type = lcm.crime_type
      AND dcm.snapshot_date = lcm.max_date
      """
      ).load()

//    val dim_living_wage = spark.read
//      .format(SNOWFLAKE_SOURCE_NAME)
//      .options(sfOptions)
//      .option("query",
//      """
//      WITH latest_living_wages AS (SELECT MAX(snapshot_date) max_date FROM dim_living_wage)
//      SELECT * FROM dim_living_wage
//      WHERE snapshot_date = (SELECT max_date FROM latest_living_wages)
//      """
//      ).load()

//    val dimTypAnnSalary = spark.read
//      .format(SNOWFLAKE_SOURCE_NAME)
//      .options(sfOptions)
//      .option("query",
//      """
//      WITH latest_typ_ann_sal AS (SELECT MAX(snapshot_date) max_date FROM dim_typical_annual_salary)
//      SELECT * FROM dim_typical_annual_salary
//      WHERE snapshot_date = (SELECT max_date FROM latest_typ_ann_sal)
//      """
//      ).load()

    val dimLocation = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query", "SELECT location_id, zip_code, state, county FROM dim_location WHERE state = 'DE'")
      .load()

//    val factListing = spark.read
//      .format(SNOWFLAKE_SOURCE_NAME)
//      .options(sfOptions)
//      .option("query",
//      """
//      WITH latest_listings AS (SELECT MAX(snapshot_date) max_date FROM fact_listing)
//      SELECT
//        location_id,
//        price,
//        bathrooms,
//        bedrooms,
//        square_footage,
//        year_built
//      FROM listing
//      WHERE snapshot_date = (SELECT max_date FROM latest_listings)
//      """
//      ).load()
//      .withColumn("age_in_years", year(current_date()) - col("year_built"))
//
    // TODO: Calculate a normalized crime score for each zip code
    val dimCrimeRateWithZip = dimCrimeRate.join(dimLocation, Seq("location_id"), "inner")
    val dimCrimeRateMinMax = dimCrimeRateWithZip
      .agg(
        min(col("rate")).alias("min_rate"),
        max(col("rate")).alias("max_rate"),
      ).first
    val dimCrimeRateNormalized = dimCrimeRateWithZip
      .withColumn(
        "normalized_crime_rate",
        ((dimCrimeRateWithZip("rate") - dimCrimeRateMinMax.getDouble(0)) /
          (dimCrimeRateMinMax.getDouble(1) - dimCrimeRateMinMax.getDouble(0))) * 100
      )
      .groupBy("location_id","zip_code")
      .agg(avg(col("normalized_crime_rate")).alias("avg_normalized_crime_rate"))
      .orderBy(desc("avg_normalized_crime_rate"))

    dimCrimeRateNormalized.show(10)
    // TODO: Calculate a cost of living score for each zip code
//
//    // Calculate recommended annual salary
//    val recommendedSalaryDf = wageDf.join(householdDf, Seq("household_id"), "inner")
//      .withColumn("recommended_annual_salary", col("hourly_wage") * 40 * 52)
//
//    // Calculate average annual salary for each county
//    val avgAnnualSalaryDf = annualSalaryDf
//      .groupBy("county").agg(avg(col("salary")).alias("avg_annual_salary"))
//
//    // Calculate AVGs for selected listing columns
//    val columnNames = List("price", "bathrooms", "bedrooms", "age_in_years", "square_footage")
//    val avgCols = columnNames.map(c => avg(col(c)).alias(s"avg_$c"))
//    val listingAvgDf = listingDf.groupBy("zip_code").agg(avgCols.head, avgCols.tail: _*)
//
//    // Join in location df
//    val listingAndLocationDf = listingAvgDf.join(locationDf, Seq("zip_code"), "inner")
//    listingAndLocationDf.show()
//
//    // Join in recommended salary df
//    val listingLocationRecommendedSalaryDf = listingAndLocationDf
//      .join(recommendedSalaryDf, Seq("county"), "inner")
//
//    // Join in average salary df
//    val allWithAverageSalary = listingLocationRecommendedSalaryDf
//      .join(avgAnnualSalaryDf, Seq("county"), "inner")
//
//    allWithAverageSalary.show()
  }
}
