package etl

import config.SparkConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import java.io.{File, PrintWriter}

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

    println("\n[INFO] Starting Sessionization Analytics across October & November Datasets...")

    // 1. Read both October and November CSV files explicitly to bypass Windows globbing
    val rawDF = spark.read
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss z")
      .schema(behaviorSchema)
      .csv("data/raw/2019-Oct.csv", "data/raw/2019-Nov.csv")

    // 2. Apply cleaning operations entirely in-memory
    val cleanedDF = rawDF
      .na.drop(Seq("user_id", "user_session", "event_time"))
      .na.fill("unknown", Seq("brand", "category_code"))
      .na.fill(0.0, Seq("price"))

    // 3. Perform Analytics: Calculate Session Duration & Purchase Conversions
    println("\n[INFO] Calculating Session Metrics at Full Scale...")
    
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

    // Display the top analytics results
    sessionMetricsDF.show(20, truncate = false)

    // 4. Export processed dataset locally via standard Java I/O to bypass Windows Hadoop committers
    println("\n[INFO] Exporting processed dataset locally...")
    val results = sessionMetricsDF.limit(500).collect()
    val pw = new PrintWriter(new File("data/processed/session_summary.csv"))
    pw.write("user_id,user_session,session_duration_mins,event_count,purchase_count\n")
    results.foreach { row =>
      pw.write(s"${row.getString(0)},${row.getString(1)},${row.getDouble(2)},${row.getLong(3)},${row.getLong(4)}\n")
    }
    pw.close()
    println("[INFO] Local export complete: data/processed/session_summary.csv")

    println("[SUCCESS] Full-Scale Analytics Phase Complete.")
    spark.stop()
  }
}