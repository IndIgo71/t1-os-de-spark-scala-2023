package hw08.common.transform

import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveExtactor {
  def execute(spark: SparkSession, query: String): DataFrame = {
    val df = spark.sql(query)

    df
  }
}
