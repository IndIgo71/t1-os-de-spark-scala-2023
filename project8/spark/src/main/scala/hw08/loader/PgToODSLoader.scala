package hw08.loader

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.current_timestamp
import hw08.common.service.SparkService
import hw08.common.transform.{HiveLoader, JdbcDbTableExtractor}

object PgToODSLoader extends App {
  if (args.size != 4) {
    println("Usage: PgToODSLoader <jdbc_url> <user> <password> <schema>")
    System.exit(-1)
  }

  val spark = SparkService.getSparkSession()

  // имя колонки c timestamp
  val tsColumnName: String = "ts"

  val pgUrl = args(0)
  val pgUser = args(1)
  val pgPassword = args(2)
  // схема
  val schemaName = args(3)

  // загрузка из Postgres 3-х таблиц в Hive
  val pgTableNames = List("card", "person", "person_adress")

  pgTableNames.foreach { tableName =>
    val fullTableName = s"$schemaName.$tableName"

    val pgDf = JdbcDbTableExtractor
      .execute(spark, pgUrl, pgUser, pgPassword, tableName)
      .withColumn(tsColumnName, current_timestamp())

    HiveLoader.execute(pgDf, fullTableName)
  }
}
