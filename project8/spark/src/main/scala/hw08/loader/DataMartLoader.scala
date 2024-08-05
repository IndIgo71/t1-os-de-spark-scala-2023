package hw08.loader

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, current_timestamp, date_format, lit, round, sum, when}
import hw08.common.service.SparkService
import hw08.common.transform.{HiveExtactor, HiveLoader}

object DataMartLoader extends App {
  if (args.size != 4) {
    println("Usage: WideTableLoader <schema> <wide_table> <datamart_table> <partValue>")
    System.exit(-1)
  }

  val spark = SparkService.getSparkSession()

  // имя пользователя (схема)
  val schemaName = args(0)
  // имя колонки c timestamp
  val tsColumnName: String = "hour"
  // имя широкой таблицы
  var wideTableName = args(1)
  wideTableName = s"$schemaName.$wideTableName"
  // имя таблицы витрины
  var datamartTableName = args(2)
  datamartTableName = s"$schemaName.$datamartTableName"
  // значение партиции с которой работаем
  val partValue = lit(args(3).toLong)

  val dfWide = HiveExtactor.execute(spark, s"SELECT * FROM $wideTableName")

  val datamartDf = dfWide
    .where(col(tsColumnName) === partValue)
    .groupBy(
      when(col("amount") < 500, "i. <500")
        .when(col("amount").between(500, 1000), "ii. 500-1000")
        .when(col("amount").between(1001, 2000), "iii. 1001-2000")
        .when(col("amount").between(2001, 3000), "iv. 2001-3000")
        .when(col("amount").between(3001, 4000), "v. 3001-4000")
        .when(col("amount").between(4001, 5000), "vi. 4001-5000")
        .when(col("amount").between(5001, 6000), "vii. 5001-6000")
        .otherwise("viii. >6000").as("category")
    )
    .agg(
      count("*").as("category_count"),
      sum(when(col("gender") === "Male" && col("account") === "Has Account in foreign bank", 1).otherwise(0)).as("mans")
    )
    .withColumn("weight", round((col("category_count").cast("decimal") / sum("category_count").over(Window.partitionBy())) * 100, 2))
    .withColumn("mans_weight", round((col("mans").cast("decimal") / sum("category_count").over(Window.partitionBy("category"))) * 100, 2))
    .select("category", "weight", "mans_weight")
    .withColumn(tsColumnName, partValue)

  HiveLoader.execute(datamartDf, datamartTableName, SaveMode.Append, tsColumnName)
}
