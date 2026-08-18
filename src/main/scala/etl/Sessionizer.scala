package etl

import config.SparkConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

object Sessionizer {

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
    // Keep winutils path, but we aren't saving anything to disk this time
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    System.setProperty("HADOOP_USER_NAME", "root")

    val spark = SparkConfig.getSession("Behavioral_Analytics_Sessionizer")
    import spark.implicits._

    println("\n[INFO] Starting Sessionization Analytics...")

    // 1. Read Raw Data directly (Bypassing the Windows disk issue)
    val rawDF = spark.read
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss z")
      .schema(behaviorSchema)
      .csv("data/raw/mock_logs.csv")

    // 2. Apply cleaning operations entirely in-memory
    val cleanedDF = rawDF
      .na.drop(Seq("user_id", "user_session", "event_time"))
      .na.fill("unknown", Seq("brand", "category_code"))
      .na.fill(0.0, Seq("price"))

    // 3. Perform Analytics: Calculate Session Duration & Purchase Conversions
    println("\n[INFO] Calculating Session Metrics...")
    
    // Create a window partitioned by each unique user session
    val sessionWindow = Window.partitionBy("user_session")

    val sessionMetricsDF = cleanedDF
      .withColumn("session_start", min("event_time").over(sessionWindow))
      .withColumn("session_end", max("event_time").over(sessionWindow))
      .withColumn("event_count", count("event_type").over(sessionWindow))
      .withColumn("is_purchase", when($"event_type" === "purchase", 1).otherwise(0))
      .withColumn("purchase_count", sum("is_purchase").over(sessionWindow))
      // Calculate duration in minutes
      .withColumn("session_duration_mins", 
        round((unix_timestamp($"session_end") - unix_timestamp($"session_start")) / 60.0, 2))
      // Collapse the data down to a summary level
      .select(
        "user_id", 
        "user_session", 
        "session_duration_mins", 
        "event_count", 
        "purchase_count"
      )
      .distinct()
      .orderBy(desc("session_duration_mins"))

    // Display the final analytics table
    sessionMetricsDF.show(truncate = false)

    println("[SUCCESS] Analytics Phase Complete.")
    spark.stop()
  }
}