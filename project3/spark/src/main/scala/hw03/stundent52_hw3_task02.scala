package hw03

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object student52_hw3_task02 extends App {
  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("student52_hw3_task02")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  // ===== 2.1 =====
  val schema = StructType(
    Seq(
      StructField("employee_id", DataTypes.IntegerType, nullable = false),
      StructField("department", DataTypes.StringType, nullable = false),
      StructField("region", DataTypes.StringType, nullable = false),
      StructField("education", DataTypes.StringType),
      StructField("gender", DataTypes.StringType, nullable = false),
      StructField("recruitment_channel", DataTypes.StringType, nullable = false),
      StructField("no_of_trainings", DataTypes.IntegerType),
      StructField("age", DataTypes.IntegerType),
      StructField("previous_year_rating", DataTypes.IntegerType),
      StructField("length_of_service", DataTypes.IntegerType),
      StructField("KPIs_met_80", DataTypes.IntegerType, nullable = false),
      StructField("awards_won", DataTypes.IntegerType, nullable = false),
      StructField("avg_training_score", DataTypes.IntegerType, nullable = false)
    )
  )

  val hrDataDf = spark
    .read
    .option("header", "true")
    .schema(schema)
    .csv("src/main/resources/HR_data.csv")

  hrDataDf.printSchema()
  hrDataDf.show(10, false)

  // ===== 2.2 =====
  val rankedRatingDf = hrDataDf
    .withColumn("education", coalesce(col("education"), lit("No info")))
    .withColumn("rank", dense_rank().over(Window.partitionBy($"education", $"gender").orderBy(desc("avg_training_score"))))

  rankedRatingDf.show(50, false)

  //   ===== 2.3 =====
  val ageGroupsDf = hrDataDf
    .withColumn("education", coalesce(col("education"), lit("No info")))
    .groupBy($"department", $"education", $"gender")
    .agg(
      min($"age").as("min_age"),
      max($"age").as("max_age"),
      round(avg($"age"), 2).as("avg_age")
    )
    .orderBy($"department", $"education", $"gender")

  ageGroupsDf.show(50, false)


  //   ===== 2.4 =====
  final case class HRData(
                           employee_id: Int,
                           department: String,
                           region: String,
                           education: Option[String],
                           gender: String,
                           recruitment_channel: String,
                           no_of_trainings: String,
                           age: Int,
                           previous_year_rating: Option[Int],
                           KPIs_met_80: Int,
                           awards_won: Int,
                           avg_training_score: Int
                         )

  final case class departmentAgeGroup(
                                       department: String,
                                       education: String,
                                       gender: String,
                                       min_age: Int,
                                       max_age: Int,
                                       avg_age: Double
                                     )

  val hrDataDataset = hrDataDf.as[HRData]
  val ageGroupsDataset = hrDataDataset
    .groupByKey(data => (data.department, data.education.getOrElse("No info"), data.gender))
    .agg(
      min("age").as("min_age").as[Int],
      max("age").as("max_age").as[Int],
      round(avg("age"), 2).as("avg_age").as[Double]
    )
    .map(
      data => departmentAgeGroup(data._1._1, data._1._2, data._1._3, data._2, data._3, data._4)
    )
    .orderBy($"department", $"education", $"gender")

  ageGroupsDataset.show(50, false)
}
