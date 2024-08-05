package hw05.transform

import org.apache.spark.sql.{DataFrame, SparkSession}

object JdbcDbTableExtractor {
  def execute(spark: SparkSession, tableName: String): DataFrame = {
    val df = spark
      .read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://db_address:5432/db_name")
      .option("dbtable", tableName)
      .option("user", "user")
      .option("password", "password")
      .load()

    df
  }
}
