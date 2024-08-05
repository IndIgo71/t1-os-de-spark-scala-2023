package hw07

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object student45_hw07 extends App {
  val appName = "student45.hw07"

  val spark = SparkSession.builder()
    .appName(appName)
    .master("local[3]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  spark.conf.set("spark.sql.adaptive.enabled", false)

  import spark.implicits._

  // генерируем случаный набор фруктов, который будем использовать в дальнейшем
  val fruits = DataGenerator.generateUniqueFruits()
  val numPartitions = fruits.size

  // отзывы по отзывам на фрукты
  val reviews = DataGenerator.generateReviews(300000, fruits)
  val dfReviews = reviews.toDF

  // продажи фруктов
  val sales = DataGenerator.generateSales(2000000, fruits)
  val dfSales = sales.toDF

  // джоин с перекосом
  dfSales.join(dfReviews, Seq("name"), "inner").count()

  val dfLeft = dfSales.withColumn("saltKey", (rand() * numPartitions))
  val dfRight = dfReviews.withColumn("saltKey", (rand() * numPartitions))

  // убираем перекос через соление
  val dfSaltSkew = dfLeft.join(dfRight, Seq("saltKey"), "inner")
  dfSaltSkew.count()

  System.in.read()
}
