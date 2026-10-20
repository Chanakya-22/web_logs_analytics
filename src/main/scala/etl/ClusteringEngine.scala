package etl

import config.SparkConfig
import java.io.{File, IOException, PrintWriter}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}

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
    // Data successfully loaded

    val assembler = new VectorAssembler()
      .setInputCols(Array("session_duration_mins", "event_count", "purchase_count"))
      .setOutputCol("raw_features")

    val assembledDF = assembler.transform(summaryDF)

    val scaler = new StandardScaler()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(true)

    val scalerModel = scaler.fit(assembledDF)
    val scaledDF = scalerModel.transform(assembledDF)

    scaledDF.show(5)

    val kmeans = new KMeans()
      .setK(4)
      .setSeed(1L)
      .setFeaturesCol("features")

    val kmeansModel = kmeans.fit(scaledDF)
    val predictions = kmeansModel.transform(scaledDF)

    kmeansModel.clusterCenters.zipWithIndex.foreach { case (center, index) =>
      println(s"Cluster center $index: ${center.toArray.mkString(", ")}")
    }

    predictions.groupBy("prediction").count().show()

    val evaluator = new ClusteringEvaluator()
      .setMetricName("silhouette")

    val silhouetteScore = evaluator.evaluate(predictions)
    println(s"Silhouette Score: $silhouetteScore")

    val outputFile = new File("data/output/cohort_assignments.csv")
    val writer = new PrintWriter(outputFile)
    try {
      writer.println("user_id,user_session,prediction")
      predictions
        .select("user_id", "user_session", "prediction")
        .limit(1000)
        .collect()
        .foreach { row =>
          val csvValues = row.toSeq.map {
            case null => ""
            case value =>
              val text = value.toString
              if (text.exists(char => char == ',' || char == '"' || char == '\n' || char == '\r')) {
                "\"" + text.replace("\"", "\"\"") + "\""
              } else {
                text
              }
          }
          writer.println(csvValues.mkString(","))
        }
    } finally {
      writer.close()
    }
    if (writer.checkError()) {
      throw new IOException(s"Failed to write cohort assignments to ${outputFile.getPath}")
    }

    spark.stop()
  }
}