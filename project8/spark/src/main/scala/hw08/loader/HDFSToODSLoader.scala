package hw08.loader

import org.apache.spark.sql.functions.current_timestamp
import hw08.common.service.SparkService
import hw08.common.transform.HiveLoader

object HDFSToODSLoader extends App {
  if (args.size != 3) {
    println("Usage: HDFSToODSLoader <schema> <table> <path_to_csv>")
    System.exit(-1)
  }

  val spark = SparkService.getSparkSession()

  // схема
  val schemaName = args(0)
  // таблица
  val tableName = args(1)
  // путь к исходным csv-файлам
  val sourceCSVPath = args(2)
  // имя колонки c timestamp
  val tsColumnName: String = "ts"

  val csvDf = spark
    .read
    .option("header", "true")
    .csv(sourceCSVPath)
    .withColumn(tsColumnName, current_timestamp())

  // загрузка сsv-фалов из HDFS в таблицу Hive
  HiveLoader.execute(csvDf, s"$schemaName.$tableName")
}
