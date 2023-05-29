# An ETL Spark job that populates a summary table in Snowflake

## Development workflow

To build the fat jar

`sbt assembly`

To run the spark job locally

```
% /Users/aidanfarhi/Software/spark-3.3.2-bin-hadoop3/bin/spark-submit --class App \
--master "local[4]" \
/Users/aidanfarhi/Development/location-iq/location-summary-etl/target/scala-2.12/location-summary-etl-LATEST.jar \
SNOWFLAKE_URL \
SNOWFLAKE_USERNAME \
SNOWFLAKE_PASSWORD \
SNOWFLAKE_DATABASE \
SNOWFLAKE_SCHEMA \
SNOWFLAKE_WAREHOUSE
```