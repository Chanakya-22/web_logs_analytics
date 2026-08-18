package etl

import config.SparkConfig
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

object DataIngestion {

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
    // 1. Point Spark to the new winutils installation
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    System.setProperty("HADOOP_USER_NAME", "root")
    
    // --- THE CRUCIAL FIX: Force pure Java IO to bypass the Windows UnsatisfiedLinkError ---
    System.setProperty("hadoop.io.nativeio.NativeIO$Windows.access", "false")

    val spark = SparkConfig.getSession("Behavioral_ETL_Engine")
    import spark.implicits._

    println("\n[INFO] Connecting to Distributed HDFS Cluster...")

    // 1. Read locally for compute
    val rawDataPath = "data/raw/mock_logs.csv"

    val rawDF: DataFrame = spark.read
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss z")
      .schema(behaviorSchema)
      .csv(rawDataPath)

    // 2. Execute Preprocessing Operations (Requirement 5)
    val cleanedDF = rawDF
      .na.drop(Seq("user_id", "user_session", "event_time"))
      .na.fill("unknown", Seq("brand", "category_code"))
      .na.fill(0.0, Seq("price"))

    cleanedDF.show(truncate = false)

    // 3. Store the Processed Dataset locally (Requirements 6 & 7 prep)
    val processedPath = "data/processed/cleaned_logs.parquet"
    
    println(s"\n[INFO] Writing processed dataset locally to: $processedPath")
    
    cleanedDF.write
      .mode("overwrite")
      .parquet(processedPath)

    println("[SUCCESS] Pipeline Execution Complete.")
    spark.stop()
  }
}