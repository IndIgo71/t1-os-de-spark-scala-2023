package hw08.loader

import org.apache.spark.sql.functions.{col, current_timestamp, date_format, lit}
import hw08.common.service.SparkService

object DataMartToPGLoader extends App {
  if (args.size != 6) {
    println("Usage: DataMartToPGLoader <jdbc_url> <user> <password> <schema> <table> <partValue>")
    System.exit(-1)
  }

  val spark = SparkService.getSparkSession()

  val url = args(0)
  val user = args(1)
  val password = args(2)


  // имя пользователя (схема)
  val schemaName = args(3)
  // имя таблицы витрины
  var datamartTableName = args(4)
  datamartTableName = s"$schemaName.$datamartTableName"

  val partValue = lit(args(5).toLong)

  val dataMartDf = spark.table(datamartTableName).where(col("hour") === partValue)

  dataMartDf
    .write
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", url)
    .option("dbtable", datamartTableName)
    .option("user", user)
    .option("password", password)
    .mode("append")
    .save()
}
