package etl

import config.SparkConfig

object ClusteringEngine {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    System.setProperty("HADOOP_USER_NAME", "root")

    val spark = SparkConfig.getSession("Behavioral_Clustering_Engine")

    val summaryDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/processed/session_summary.csv")

    summaryDF.printSchema()
    summaryDF.show(5)

    spark.stop()
  }
}