import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.functions.{avg, col, current_date, datediff, lit, max, min, round, when, year}

import java.text.SimpleDateFormat
import java.util.Date

object App extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {

    val dimCrimeRate = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query",
      """
      WITH latest_crime_rates AS (
        SELECT
          location_id,
          crime_type,
          MAX(snapshot_date) AS max_date
        FROM dim_crime_rate
        GROUP BY location_id, crime_type
      )
      SELECT
        dcm.location_id,
        dcm.rate,
        dcm.crime_type
      FROM dim_crime_rate dcm
      JOIN latest_crime_rates lcm
      ON
        dcm.location_id = lcm.location_id AND
        dcm.crime_type = lcm.crime_type AND
        dcm.snapshot_date = lcm.max_date
      """
      ).load()

    val dimLivingWage = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query",
      """
      WITH latest_living_wages AS (SELECT MAX(snapshot_date) max_date FROM dim_living_wage)
      SELECT
        location_id,
        hourly_wage
      FROM dim_living_wage
      WHERE
        snapshot_date = (SELECT max_date FROM latest_living_wages) AND
        number_of_adults = 2 AND
        number_of_children = 2 AND
        number_of_working_adults = 1
      """
      ).load()

    val dimTypAnnSalary = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query",
      """
      WITH latest_typ_ann_sal AS (SELECT MAX(snapshot_date) max_date FROM dim_typical_annual_salary)
      SELECT * FROM dim_typical_annual_salary
      WHERE snapshot_date = (SELECT max_date FROM latest_typ_ann_sal)
      """
      ).load()

    val dimLocation = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query",
      """
      SELECT
        location_id,
        zip_code,
        state,
        county
      FROM dim_location
      WHERE state = 'DE'
      """
      ).load()

    val dimAnnualExpense = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query",
        """
        WITH latest_annual_expense AS (SELECT MAX(snapshot_date) max_date FROM dim_annual_expense)
        SELECT
          location_id,
          amount
        FROM dim_annual_expense
        WHERE
          snapshot_date = (SELECT max_date FROM latest_annual_expense) AND
          number_of_adults = 2 AND
          number_of_children = 2 AND
          number_of_working_adults = 1
          """
      ).load()

    val factListing = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query",
      """
      WITH latest_listings AS (SELECT MAX(snapshot_date) max_date FROM fact_listing)
      SELECT
        location_id,
        price,
        bathrooms,
        bedrooms,
        square_footage,
        listed_date,
        removed_date,
        year_built
      FROM fact_listing
      WHERE snapshot_date = (SELECT max_date FROM latest_listings)
      """
      ).load()
      .withColumn("age_in_years", year(current_date()) - col("year_built"))
      .withColumn("days_on_market",
        when(col("removed_date").isNull, datediff(current_date(), col("listed_date")))
        .otherwise(datediff(col("removed_date"), col("listed_date"))))

    val dimCrimeRateWithZip = dimCrimeRate.join(dimLocation, Seq("location_id"), "inner")
    val dimCrimeRateAvgRate = dimCrimeRateWithZip
      .groupBy("location_id", "zip_code")
      .agg(avg(col("rate")).alias("avg_crime_rate"))
    val dimCrimeRateMinMax = dimCrimeRateAvgRate
      .agg(
        min(col("avg_crime_rate")).alias("min_rate"),
        max(col("avg_crime_rate")).alias("max_rate"),
      ).first
    val dimCrimeRateNormalized = dimCrimeRateAvgRate
      .withColumn(
        "normalized_crime_rate",
        ((dimCrimeRateAvgRate("avg_crime_rate") - dimCrimeRateMinMax.getDouble(0)) /
          (dimCrimeRateMinMax.getDouble(1) - dimCrimeRateMinMax.getDouble(0))) * 100
      )

    val dimExpenseWithZip = dimAnnualExpense.join(dimLocation, Seq("location_id"), "inner")
    val dimExpenseAvgExpense = dimExpenseWithZip
      .groupBy("location_id", "zip_code")
      .agg(avg(col("amount")).alias("avg_expense"))
    val dimExpenseMinMax = dimExpenseAvgExpense
      .agg(
        min(col("avg_expense")).alias("min_rate"),
        max(col("avg_expense")).alias("max_rate"),
      ).first
    val dimExpenseNormalized = dimExpenseAvgExpense
      .withColumn(
        "normalized_expense",
        ((dimExpenseAvgExpense("avg_expense") - dimExpenseMinMax.getDouble(0)) /
          (dimExpenseMinMax.getDouble(1) - dimExpenseMinMax.getDouble(0))) * 100
      )

    // Calculate recommended annual salary
    val recommendedAnnSalary = dimLivingWage
      .withColumn("RECOMMENDED_ANNUAL_SALARY", col("hourly_wage") * 40 * 52)

    // Calculate average annual salary
    val avgAnnualSalary = dimTypAnnSalary
      .groupBy("location_id").agg(avg(col("salary")).alias("AVERAGE_ANNUAL_SALARY"))

    // Calculate AVGs for selected listing columns
    val columnNames = List("price", "bathrooms", "bedrooms", "age_in_years", "square_footage", "days_on_market")
    val avgCols = columnNames.map(c => avg(col(c)).alias(s"avg_$c"))
    val listingSummary = factListing.groupBy("location_id").agg(avgCols.head, avgCols.tail: _*)

    // Join in location df
    val listingLocation = listingSummary.join(dimLocation, Seq("location_id"), "inner")

    // Join in recommended salary
    val listingLocRecSal = listingLocation
      .join(recommendedAnnSalary, Seq("location_id"), "inner")

    // Join in average salary df
    val listingLocRecSalAvgSal = listingLocRecSal
      .join(avgAnnualSalary, Seq("location_id"), "inner")

    val listingLocRecSalAvgSalCrimeScore = listingLocRecSalAvgSal
      .join(dimCrimeRateNormalized, Seq("location_id"), "inner")

    val rawFinalResult = listingLocRecSalAvgSalCrimeScore
      .join(dimExpenseNormalized, Seq("location_id"), "inner")
      .withColumn(
        "AVERAGE_PRICE_PER_SQUARE_FOOT",
        col("avg_price") / col("avg_square_footage")
      )

    // TODO: Figure out how to get only one ZIP_CODE col

    val finalColsStrings = List(
      "STATE", "COUNTY", "RECOMMENDED_ANNUAL_SALARY", "AVERAGE_ANNUAL_SALARY",
      "EXPENSE_SCORE", "CRIME_SCORE", "AVERAGE_HOME_PRICE", "AVERAGE_HOME_AGE_IN_YEARS",
      "AVERAGE_SQUARE_FOOTAGE", "AVERAGE_PRICE_PER_SQUARE_FOOT", "SNAPSHOT_DATE",
      "AVERAGE_TIME_ON_MARKET_IN_DAYS"
    )
    val finalCols = finalColsStrings.map(s => col(s))

    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateFormatter.format(new Date())
    val finalResult = rawFinalResult
      .withColumnRenamed("normalized_expense", "EXPENSE_SCORE")
      .withColumnRenamed("normalized_crime_rate", "CRIME_SCORE")
      .withColumnRenamed("avg_price", "AVERAGE_HOME_PRICE")
      .withColumnRenamed("avg_age_in_years", "AVERAGE_HOME_AGE_IN_YEARS")
      .withColumnRenamed("avg_square_footage", "AVERAGE_SQUARE_FOOTAGE")
      .withColumn("AVERAGE_HOME_PRICE", round(col("AVERAGE_HOME_PRICE"), 2))
      .withColumn("AVERAGE_HOME_AGE_IN_YEARS", round(col("AVERAGE_HOME_AGE_IN_YEARS"), 2))
      .withColumn("AVERAGE_SQUARE_FOOTAGE", round(col("AVERAGE_SQUARE_FOOTAGE"), 2))
      .withColumn("AVERAGE_ANNUAL_SALARY", round(col("AVERAGE_ANNUAL_SALARY"), 2))
      .withColumn("EXPENSE_SCORE", round(col("EXPENSE_SCORE"), 2))
      .withColumn("CRIME_SCORE", round(col("CRIME_SCORE"), 2))
      .withColumn("AVERAGE_TIME_ON_MARKET_IN_DAYS", round(col("avg_days_on_market"), 2))
      .withColumn("AVERAGE_PRICE_PER_SQUARE_FOOT", round(col("AVERAGE_PRICE_PER_SQUARE_FOOT"), 2))
      .withColumn("SNAPSHOT_DATE", lit(today))
      .select(finalCols:_*)

    finalResult.show()
  }
}
