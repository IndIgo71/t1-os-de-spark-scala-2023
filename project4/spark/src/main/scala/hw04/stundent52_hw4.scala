package hw04

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes}

object stundent52_hw4 extends App {
  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("student45_lecture04")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  spark.conf.set("spark.sql.adaptive.enabled", false)

  import spark.implicits._

  // Задание 1
  /*
  Согласно представленному в задании 1 физическому плану,
  предполагаемый код может выглядеть следующим образом:

    val task1_df = spark
      .read
      .option("header", "true")
      .csv("path/to/file/country_info.csv")
      .groupBy("country")
      .agg(
        count("*"),
        max("name")
      )
      .repartition(10)
  */

  // Подготовка датафреймов с исходными данными для заданий 2-3
  val artistsDf = spark
    .read
    .option("header", "true")
    .csv("src/main/resources/Artists.csv")

  val topSongsDf = spark
    .read
    .option("header", "true")
    .csv("src/main/resources/Top_Songs_US.csv")

  // Задание 2
  val resultDf = artistsDf.alias("ar")
    .join(topSongsDf.as("ts"), $"ts.`Artist ID`" === $"ar.ID", "inner")
    .filter($"ar.Followers" > 50000000)
    .groupBy($"ar.Name")
    .agg(
      sum($"ts.`Song Duration`").alias("Total Duration")
    )
    .withColumn("Total Duration", $"`Total Duration`".cast(DataTypes.LongType))
    .select(
      $"Name",
      $"`Total Duration`"
    )

  resultDf.show(truncate = false)

  /*
  Описание оптимизаций:
    1. predicate pushdown (выполнение фильтрации на более ранних этапах, с целью уменьшения размера выборки данных):
      - фильтрация `Artist ID` is not null
      - фильтрация `ID` is not null
      - фильрация `Followers` > 50000000
    2. projection prunning (проекция столбцов - считывание только необходимых столбцов для конечного результата):
      - `Artist ID` и `Song Duration`
      - `ID`, `Name` и `Followers`
    3. join reordering (изменение порядка соединений датафреймов)
      - использование стратегии соединений Sort Merge Join
      (поскольку отключена автоматическая трансляция по всем узлам таблиц, указанного размера)
  */


  // Задание 3
  val resultBroadcastDf = broadcast(artistsDf).alias("ar")
    .join(topSongsDf.as("ts"), $"ts.`Artist ID`" === $"ar.ID", "inner")
    .filter($"ar.Followers" > 50000000)
    .groupBy($"ar.Name")
    .agg(
      sum($"ts.`Song Duration`").alias("Total Duration")
    )
    .withColumn("Total Duration", $"`Total Duration`".cast(DataTypes.LongType))
    .select(
      $"Name",
      $"`Total Duration`"
    )

  resultBroadcastDf.show(truncate = false)

  /*
  Описание оптимизаций:
    1. predicate pushdown (выполнение фильтрации на более ранних этапах, с целью уменьшения размера выборки данных):
      - фильтрация `Artist ID` is not null
      - фильтрация `ID` is not null
      - фильрация `Followers` > 50000000
    2. projection prunning (проекция столбцов - считывание только необходимых столбцов для конечного результата):
      - `Artist ID` и `Song Duration`
      - `ID`, `Name` и `Followers`
    3. join reordering (изменение порядка соединений датафреймов)
      - использование стратегии соединений Broadcast Join
      Поскольку, в явном виде указано использование broadcast для таблицы меньшего размера,
      то Catalyst переместить ее на все узлы кластера.
      По сравнению с п.2, данная стратегия соеденения уменьшает количество сортировок и перемещений по узлам таблицы меньшего размера (указанной в качестве).
      В следствие этого уменьшается и общее время выполнения программы в сравнении с временем программы из п.2.
  */

  System.in.read()
}
