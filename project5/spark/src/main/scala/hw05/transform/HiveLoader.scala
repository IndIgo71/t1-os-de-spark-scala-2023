package hw05.transform

import org.apache.spark.sql.{DataFrame, SaveMode}

object HiveLoader {
  def execute(df: DataFrame, tableName: String): Unit = {
    df.repartition(10).write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    df.sparkSession.catalog.refreshTable(tableName)
  }
}
