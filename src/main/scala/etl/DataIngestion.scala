package etl

import config.SparkConfig
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object DataIngestion {

  // 1. Define the rigid schema to bypass expensive inference on massive datasets
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
    // 2. Boot up the local engine using our optimized config
    val spark = SparkConfig.getSession("Behavioral_ETL_Engine")
    
    // Allows us to use shorthand for column names (e.g., $"column_name")
    import spark.implicits._

    println("\n[INFO] Spark Execution Engine Initialized.")
    println("[INFO] Commencing Type-Safe Data Ingestion...\n")

    // 3. Load the data using the enforced schema
    // This relative path assumes you are executing from the repository root
    val rawDataPath = "data/raw/mock_logs.csv"

    val rawDF: DataFrame = spark.read
      .option("header", "true")
      // Spark sometimes struggles with UTC strings; this forces standard parsing
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss z") 
      .schema(behaviorSchema)
      .csv(rawDataPath)

    // 4. Data Cleaning Pipeline
    val cleanedDF = rawDF
      // Drop any rows missing the fundamental identifiers needed for sessionization
      .na.drop(Seq("user_id", "user_session", "event_time")) 
      // Impute missing categorical data rather than dropping valuable behavioral clicks
      .na.fill("unknown", Seq("brand", "category_code"))
      // Fill missing prices with 0.0 to prevent DoubleType math errors later
      .na.fill(0.0, Seq("price"))

    // 5. Output Verification
    println("=== Strict Schema Enforcement ===")
    cleanedDF.printSchema()

    println("=== Cleaned Data Preview ===")
    cleanedDF.show(truncate = false)

    // Graceful shutdown to release system memory
    spark.stop()
  }
}