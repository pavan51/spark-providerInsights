import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._

object ProviderRoster {

  // Define case classes for schemas
  case class Provider(provider_id: String, provider_specialty: String, first_name: String, middle_name: String, last_name: String)
  case class Visit(visit_id: Int, provider_id: Int, date_of_service: String)

  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("Provider Visits with Case Class")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Set log level to INFO
    spark.sparkContext.setLogLevel("INFO")

    // Read data into datasets
    val providersDS: Dataset[Provider] = spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .csv("./data/providers.csv")
      .as[Provider]

    val visitsDF = spark.read
      .option("header", "false")        // No header in the file
      .option("inferSchema", "true")    // Automatically infer column types
      .csv("./data/visits.csv")        // Replace with the actual path

//    val visitsDS1: Dataset[Visit] = spark.read
//      .option("header", "false")
//      .option("inferSchema", "true")
//      .csv("./data/visits.csv")
//      .as[Visit]

    val visitsDS = visitsDF.toDF("visit_id", "provider_id", "date_of_service")

    providersDS.printSchema()
    visitsDS.printSchema()

    // Problem 1: Total number of visits per provider
    val visitsPerProvider = visitsDS.groupBy("provider_id")
      .agg(count("*").alias("total_visits"))

    val result1 = providersDS.join(visitsPerProvider, "provider_id")
      .select(
        col("provider_id"),
        concat_ws(" ", col("first_name"), col("middle_name"), col("last_name")).alias("provider_name"),
        col("provider_specialty"),
        col("total_visits")
      )

    // Write result partitioned by provider specialty
    result1.show(10)
    result1.write
      .mode("overwrite")
      .partitionBy("provider_specialty")
      .json("output/visits_per_provider")

    // Problem 2: Total visits per provider per month
    val visitsWithMonth = visitsDS.withColumn(
      "month", date_format(col("date_of_service"), "yyyy-MM")
    )

    val visitsPerProviderPerMonth = visitsWithMonth.groupBy("provider_id", "month")
      .agg(count("*").alias("total_visits_per_month"))

    val result2 = visitsPerProviderPerMonth.select("provider_id", "month", "total_visits_per_month")
    result2.show(10)
    // Write result to JSON
    result2.write.mode("overwrite").json("output/visits_per_provider_per_month")

    // Stop the Spark session
    spark.stop()
  }
}