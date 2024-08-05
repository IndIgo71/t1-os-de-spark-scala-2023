package hw06

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}

object student45_hw06 extends App {
  val appName = "student45.hw06.TableRowsCount"

  val spark = SparkSession.builder()
    .appName(appName)
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  if (args.length != 3) {
    println("Usage: student45_hw06 <schema_name> <table_name> <hdfsResultPath>")
    System.exit(1)
  }

  val schemaName = args(0)
  val tableName = args(1)
  val outputPath = args(2)

  val rowCount = spark.table(s"$schemaName.$tableName").count()

  val rowCountRDD = spark.sparkContext.parallelize(Seq(rowCount))

  val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val path = new Path(outputPath)
  if (fileSystem.exists(path)) {
    fileSystem.delete(path, true)
  }

  rowCountRDD.coalesce(1).saveAsTextFile(outputPath)
}
