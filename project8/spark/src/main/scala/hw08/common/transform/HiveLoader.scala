package hw08.common.transform

import org.apache.spark.sql.{DataFrame, SaveMode}

object HiveLoader {
  def execute(df: DataFrame, tableName: String, mode: SaveMode = SaveMode.Overwrite, partitionColumns: String = ""): Unit = {
    val writer = df.write.mode(mode)
    if (partitionColumns.nonEmpty) {
      writer.partitionBy(partitionColumns)
    }
    writer.saveAsTable(tableName)
    df.sparkSession.catalog.refreshTable(tableName)
  }
}
