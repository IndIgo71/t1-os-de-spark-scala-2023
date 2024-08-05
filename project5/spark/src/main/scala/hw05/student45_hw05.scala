package hw05

import org.apache.spark.sql.DataFrame
import hw05.common.service._
import hw05.ransform.{HiveExtactor, HiveLoader, JdbcDbTableExtractor}


object student52_hw05 extends App {
  val appName = "student45_hw05"
  val spark = SparkService.getSparkSession(appName)

  val studentDbName = "student45"
  val tableNames = List("card", "person", "person_adress")
  var dfList: List[DataFrame] = Nil

  tableNames.foreach { tableName =>
    val tableNameWithDbName = s"$studentDbName.$tableName"

    val dfJDBC = JdbcDbTableExtractor.execute(spark, tableName)
    HiveLoader.execute(dfJDBC, tableNameWithDbName)

    val dfHive = HiveExtactor.execute(spark, s"SELECT * FROM $tableNameWithDbName")
    dfList = dfList :+ dfHive
  }

  import spark.implicits._

  val List(dfCard, dfPerson, dfPersonAddress) = dfList

  val dfPersonInfo = dfPerson.alias("p")
    .join(dfCard.alias("c"), Seq("guid"))
    .join(dfPersonAddress.alias("pa"), Seq("guid"))

  val dfPersonInfoFiltered = dfPersonInfo
    .filter(
      $"gender" === "Female"
        && $"age" > 50
        && $"province" === "QC"
        && $"amount" > 3000
        && $"zip".startsWith("8")
    )

  HiveLoader.execute(dfPersonInfo, s"$studentDbName.person_info")
  HiveLoader.execute(dfPersonInfoFiltered, s"$studentDbName.person_info_filtered")
}
