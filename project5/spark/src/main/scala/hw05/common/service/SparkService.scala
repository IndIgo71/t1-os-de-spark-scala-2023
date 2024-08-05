package hw05.common.service

import org.apache.spark.sql.SparkSession

object SparkService {
  def getSparkSession(appName: String): SparkSession = {
    val spark = SparkSession.builder()
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }

}
