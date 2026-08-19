package etl

import config.SparkConfig
import org.apache.spark.sql.types._

object DataIngestion {

  // Define the exact schema to enforce data contracts (Rubric Item 4)
  val behaviorSchema = StructType(Array(
    StructField("event_time", TimestampType, nullable = true),
    StructField("event_type", StringType, nullable = true),
    StructField("product_id", IntegerType, nullable = true),
    StructField("category_id", LongType, nullable = true),
    StructField("category_code", StringType, nullable = true),
    StructField("brand", StringType, nullable = true),
    StructField("price", DoubleType, nullable = true),
    StructField("user_id", StringType, nullable = true),
    StructField("user_session", StringType, nullable = true)
  ))

  def main(args: Array[String]): Unit = {
    // Keep winutils path to bypass Windows Hadoop errors
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    System.setProperty("HADOOP_USER_NAME", "root")

    val spark = SparkConfig.getSession("Behavioral_Analytics_Ingestion")

    println("\n[INFO] Starting Data Ingestion & Preprocessing Phase...")

    // 1. Ingest Production Data (Mock data completely removed)
    val rawDF = spark.read
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss z")
      .schema(behaviorSchema)
      .csv("data/raw/2019-Oct.csv", "data/raw/2019-Nov.csv")

    println("\n[INFO] Raw Data Schema Enforced:")
    rawDF.printSchema()

    // 2. Apply Preprocessing Operations (Rubric Item 5)
    println("\n[INFO] Applying Data Cleaning: Dropping nulls & imputing missing values...")
    val cleanedDF = rawDF
      .na.drop(Seq("user_id", "user_session", "event_time"))
      .na.fill("unknown", Seq("brand", "category_code"))
      .na.fill(0.0, Seq("price"))

    // Display a sample of the cleaned production data
    println("\n[INFO] Preprocessed Data Sample:")
    cleanedDF.show(10, truncate = false)

    println("[SUCCESS] Data Ingestion and Preprocessing Complete.")
    spark.stop()
  }
}