package com.javi.personal.tfg.processors.utils

import com.javi.personal.tfg.processors.processor.ProcessedTables
import com.javi.personal.tfg.processors.{PathBuilder, StorageAccountLocation}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

class DefaultDataSourceProvider extends DataSourceProvider {

  override def readSilver(source: String, datasetName: String)(implicit spark: SparkSession): DataFrame =
    read(PathBuilder.buildSilverPath(source, datasetName))

  override def readSilver(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): DataFrame =
    read(PathBuilder.buildSilverPath(source, datasetName).cd(date))

  override def readSilverOption(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): Option[DataFrame] =
    readOption(PathBuilder.buildSilverPath(source, datasetName).cd(date))

  override def readGold(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): DataFrame = {
    val location = dateOption.map(date => PathBuilder.buildGoldPath(dataset.getName).cd(date))
      .getOrElse(PathBuilder.buildGoldPath(dataset.getName))
    read(location)
  }

  override def readGoldOption(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): Option[DataFrame] = {
    val location = dateOption.map(date => PathBuilder.buildGoldPath(dataset.getName).cd(date))
      .getOrElse(PathBuilder.buildGoldPath(dataset.getName))
    readOption(location)
  }

  private def read(location: StorageAccountLocation, format: String = "parquet")(implicit spark: SparkSession): DataFrame =
    spark.read.format(format).option("mergeSchema", "true").load(location.url)

  private def readOption(location: StorageAccountLocation, format: String = "parquet")(implicit spark: SparkSession): Option[DataFrame] =
    try Some(read(location, format)) catch { case _: Exception => None }

}
