package hw03

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession}

object student52_hw3_task01 extends App {
  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("student52_hw3_task01")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val mock_data = Seq(
    (1, "Tracey"),
    (2, "Mandi"),
    (3, "Georgina"),
    (4, "Vasily"),
    (5, "Cristiano"),
    (6, null),
    (7, "Clara"),
    (8, "Ninette"),
    (9, "Melisse"),
    (10, "Marijo")
  )

  final case class Person(id: Int, name: Option[String])

  val ds = mock_data.toDF("id", "name").as[Person]

  //  // вариант изменения поля name через DataFrame (Dataset[Row])
  //  val modifiedDs = ds
  //    .withColumn(
  //      "name",
  //      when($"name".isNotNull, concat($"name", lit("-"), $"id")).otherwise(concat(lit("-"), $"id")))
  //    .as[Person]

  // вариант изменения поля name через Dataset[Person\
  val modifiedDs = ds
    .map(
      person => {
        val newName = person.name.map(_ + "-" + person.id).getOrElse("-" + person.id)
        Person(person.id, Option(newName))
      }
    )
  modifiedDs.show()

}
