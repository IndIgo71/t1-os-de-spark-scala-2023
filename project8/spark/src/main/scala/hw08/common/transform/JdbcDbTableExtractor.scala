package hw08.common.transform

import org.apache.spark.sql.{DataFrame, SparkSession}

object JdbcDbTableExtractor {
  def execute(spark: SparkSession, url: String, user: String, password: String, tableName: String): DataFrame = {
    val df = spark
      .read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", url)
      .option("dbtable", tableName)
      .option("user", user)
      .option("password", password)
      .load()

    df
  }
}
