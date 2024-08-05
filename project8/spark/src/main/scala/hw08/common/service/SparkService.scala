package hw08.service

import org.apache.spark.sql.SparkSession

object SparkService {
  def getSparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }

}
