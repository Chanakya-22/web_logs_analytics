package config

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object SparkConfig {
  def getSession(appName: String): SparkSession = {
    // Suppress overwhelming warning logs in terminal output
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    SparkSession.builder()
      .appName(appName)
      .master("local[*]") // Utilize all available CPU threads automatically
      .config("spark.driver.memory", "22g") // Reserve 22GB for Spark driver out of 32GB total
      .config("spark.sql.shuffle.partitions", "32") // Match partition count to system execution capacity
      .config("spark.memory.fraction", "0.8") // Dedicate 80% of execution heap space to active operations
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY") // Handle standard datetime format protocols cleanly
      .getOrCreate()
  }
}
