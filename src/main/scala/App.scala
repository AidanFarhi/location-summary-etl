import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, col, current_date, datediff, max, min, round, when, year}

object App {
  def main(args: Array[String]): Unit = {

    val sfOptions: Map[String, String] = Map(
        "sfURL" -> args(0),
        "sfUser" -> args(1),
        "sfPassword" -> args(2),
        "sfDatabase" -> args(3),
        "sfSchema" -> args(4),
        "sfWarehouse" -> args(5)
      )

    val conf = new SparkConf().setAppName("location-summary-etl")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    val factCrimeRate = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query",
      """
      WITH latest_crime_rates AS (
        SELECT
          cr.location_id,
          cr.crime_type,
          MAX(dd.date) AS max_date
        FROM fact_crime_rate cr
        JOIN dim_date dd
        ON dd.date_id = cr.snapshot_date_id
        GROUP BY cr.location_id, cr.crime_type
      )
      SELECT
        fcr.location_id,
        fcr.rate,
        fcr.crime_type
      FROM fact_crime_rate fcr
      JOIN dim_date dd
      ON dd.date_id = fcr.snapshot_date_id
      JOIN latest_crime_rates lcr
      ON
        fcr.location_id = lcr.location_id AND
        fcr.crime_type = lcr.crime_type AND
        dd.date = lcr.max_date
      """
      ).load()

    val factLivingWage = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query",
      """
      WITH max_living_wage_date AS (
        SELECT MAX(dd.date) max_date
        FROM fact_living_wage flw
        JOIN dim_date dd
        ON dd.date_id = flw.snapshot_date_id
      )
      SELECT
        flw.location_id,
        flw.hourly_wage
      FROM fact_living_wage flw
      JOIN dim_date dd
      ON dd.date_id = flw.snapshot_date_id
      WHERE
        dd.date = (SELECT max_date FROM max_living_wage_date) AND
        flw.number_of_adults = 2 AND
        flw.number_of_children = 2 AND
        flw.number_of_working_adults = 1
      """
      ).load()

    val factTypAnnSalary = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query",
      """
      WITH max_typ_ann_sal_date AS (
        SELECT MAX(dd.date) AS max_date
        FROM fact_typical_annual_salary ftas
        JOIN dim_date dd
        ON dd.date_id = ftas.snapshot_date_id
      )
      SELECT * FROM fact_typical_annual_salary ftas
      JOIN dim_date dd
      ON dd.date_id = ftas.snapshot_date_id
      WHERE dd.date = (SELECT max_date FROM max_typ_ann_sal_date)
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

    val factAnnualExpense = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query",
      """
      WITH annual_expense_max_date AS (
        SELECT MAX(dd.date) AS max_date
        FROM fact_annual_expense fae
        JOIN dim_date dd
        ON dd.date_id = fae.snapshot_date_id
      )
      SELECT
        fae.location_id,
        fae.amount
      FROM fact_annual_expense fae
      JOIN dim_date dd
      ON dd.date_id = fae.snapshot_date_id
      WHERE
        dd.date = (SELECT max_date FROM annual_expense_max_date) AND
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
      WITH listing_max_date AS (
        SELECT MAX(dd.date) AS max_date
        FROM fact_listing fl
        JOIN dim_date dd
        ON dd.date_id = fl.snapshot_date_id
      )
      SELECT
        fl.location_id,
        fl.price,
        fl.bathrooms,
        fl.bedrooms,
        fl.square_footage,
        sd.date,
        ld.date AS listed_date,
        rd.date AS removed_date,
        fl.year_built
      FROM fact_listing fl
      JOIN dim_date sd
      ON sd.date_id = fl.snapshot_date_id
      JOIN dim_date ld
      ON ld.date_id = fl.listed_date_id
      LEFT JOIN dim_date rd
      ON rd.date_id = fl.removed_date_id
      WHERE sd.date = (SELECT max_date FROM listing_max_date)
      """
      ).load()
      .withColumn("age_in_years", year(current_date()) - col("year_built"))
      .withColumn("days_on_market",
        when(col("removed_date").isNull, datediff(current_date(), col("listed_date")))
        .otherwise(datediff(col("removed_date"), col("listed_date"))))

    val factCrimeRateWithZip = factCrimeRate.join(dimLocation, Seq("location_id"), "inner")
    val factCrimeRateAvgRate = factCrimeRateWithZip
      .groupBy("location_id", "zip_code")
      .agg(avg(col("rate")).alias("avg_crime_rate"))
    val factCrimeRateMinMax = factCrimeRateAvgRate
      .agg(
        min(col("avg_crime_rate")).alias("min_rate"),
        max(col("avg_crime_rate")).alias("max_rate"),
      ).first
    val factCrimeRateNormalized = factCrimeRateAvgRate
      .withColumn(
        "normalized_crime_rate",
        ((factCrimeRateAvgRate("avg_crime_rate") - factCrimeRateMinMax.getDouble(0)) /
          (factCrimeRateMinMax.getDouble(1) - factCrimeRateMinMax.getDouble(0))) * 100
      )

    val factExpenseWithZip = factAnnualExpense.join(dimLocation, Seq("location_id"), "inner")
    val factExpenseAvgExpense = factExpenseWithZip
      .groupBy("location_id", "zip_code")
      .agg(avg(col("amount")).alias("avg_expense"))
    val factExpenseMinMax = factExpenseAvgExpense
      .agg(
        min(col("avg_expense")).alias("min_rate"),
        max(col("avg_expense")).alias("max_rate"),
      ).first
    val dimExpenseNormalized = factExpenseAvgExpense
      .withColumn(
        "normalized_expense",
        ((factExpenseAvgExpense("avg_expense") - factExpenseMinMax.getDouble(0)) /
          (factExpenseMinMax.getDouble(1) - factExpenseMinMax.getDouble(0))) * 100
      )

    // Calculate recommended annual salary
    val recommendedAnnSalary = factLivingWage
      .withColumn("RECOMMENDED_ANNUAL_SALARY", col("hourly_wage") * 40 * 52)

    // Calculate average annual salary
    val avgAnnualSalary = factTypAnnSalary
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
      .join(factCrimeRateNormalized, Seq("location_id"), "inner")
      .drop(listingLocRecSalAvgSal("zip_code"))

    val rawFinalResult = listingLocRecSalAvgSalCrimeScore
      .join(dimExpenseNormalized, Seq("location_id"), "inner")
      .withColumn(
        "AVERAGE_PRICE_PER_SQUARE_FOOT",
        col("avg_price") / col("avg_square_footage")
      )
      .drop(listingLocRecSalAvgSalCrimeScore("zip_code"))

    val finalColsStrings = List(
      "ZIP_CODE", "STATE", "COUNTY", "RECOMMENDED_ANNUAL_SALARY", "AVERAGE_ANNUAL_SALARY",
      "EXPENSE_SCORE", "CRIME_SCORE", "AVERAGE_HOME_PRICE", "AVERAGE_HOME_AGE_IN_YEARS",
      "AVERAGE_SQUARE_FOOTAGE", "AVERAGE_PRICE_PER_SQUARE_FOOT", "AVERAGE_TIME_ON_MARKET_IN_DAYS",
      "SNAPSHOT_DATE"
    )
    val finalCols = finalColsStrings.map(s => col(s))

    val finalResult = rawFinalResult
      .withColumnRenamed("normalized_expense", "EXPENSE_SCORE")
      .withColumnRenamed("normalized_crime_rate", "CRIME_SCORE")
      .withColumnRenamed("avg_price", "AVERAGE_HOME_PRICE")
      .withColumnRenamed("avg_age_in_years", "AVERAGE_HOME_AGE_IN_YEARS")
      .withColumnRenamed("avg_square_footage", "AVERAGE_SQUARE_FOOTAGE")
      .withColumnRenamed("zip_code", "ZIP_CODE")
      .withColumn("AVERAGE_HOME_PRICE", round(col("AVERAGE_HOME_PRICE"), 2))
      .withColumn("AVERAGE_HOME_AGE_IN_YEARS", round(col("AVERAGE_HOME_AGE_IN_YEARS"), 2))
      .withColumn("AVERAGE_SQUARE_FOOTAGE", round(col("AVERAGE_SQUARE_FOOTAGE"), 2))
      .withColumn("AVERAGE_ANNUAL_SALARY", round(col("AVERAGE_ANNUAL_SALARY"), 2))
      .withColumn("EXPENSE_SCORE", round(col("EXPENSE_SCORE"), 2))
      .withColumn("CRIME_SCORE", round(col("CRIME_SCORE"), 2))
      .withColumn("AVERAGE_TIME_ON_MARKET_IN_DAYS", round(col("avg_days_on_market"), 2))
      .withColumn("AVERAGE_PRICE_PER_SQUARE_FOOT", round(col("AVERAGE_PRICE_PER_SQUARE_FOOT"), 2))
      .withColumn("SNAPSHOT_DATE", current_date())
      .select(finalCols:_*)

    finalResult.write
      .format("snowflake")
      .options(sfOptions)
      .option("dbtable", "summary_zip_code")
      .mode(SaveMode.Append)
      .save()
  }
}
