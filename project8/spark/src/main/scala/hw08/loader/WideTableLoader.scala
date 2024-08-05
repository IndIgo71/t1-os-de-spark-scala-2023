package hw08.loader

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{current_timestamp, date_format, lit}
import hw08.common.service.SparkService
import hw08.common.transform.{HiveExtactor, HiveLoader}

object WideTableLoader extends App {
  if (args.size != 3) {
    println("Usage: WideTableLoader <schema> <wide_table> <partValue>")
    System.exit(-1)
  }

  val spark = SparkService.getSparkSession()

  // имя пользователя (схема)
  val schemaName = args(0)
  // имя результирующей таблицы
  val tableName = args(1)
  val wideTableName = s"$schemaName.$tableName"
  // имя колонки c timestamp
  val tsColumnName: String = "hour"

  val partValue = lit(args(2).toLong)

  val tableNames = List("card", "person", "person_adress")
  var dfList: List[DataFrame] = Nil

  tableNames.foreach { tableName =>
    val df = HiveExtactor.execute(spark, s"SELECT * FROM $schemaName.$tableName")
    dfList = dfList :+ df
  }

  val List(cardDf, personDf, addressDf) = dfList

  val accountDf = HiveExtactor.execute(spark, s"SELECT * FROM $schemaName.account")

  val personWideDf = personDf.drop("ts")
    .join(cardDf.drop("ts"), Seq("guid"))
    .join(addressDf.drop("ts"), Seq("guid"))
    .join(accountDf.drop("ts"), Seq("guid"))
    .withColumn(tsColumnName, partValue)

  HiveLoader.execute(personWideDf, wideTableName, SaveMode.Append, tsColumnName)
}
