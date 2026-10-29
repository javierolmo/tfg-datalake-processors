package com.javi.personal.tfg.processors.processor.etls

import com.javi.personal.tfg.processors.processor.etls.PisosProperties._
import com.javi.personal.tfg.processors.processor.transformers.PisosTransformer
import com.javi.personal.tfg.processors.processor.{ETL, ProcessedTables, Processor, ProcessorConfig}
import com.javi.personal.tfg.processors.utils.writers.SparkWriter
import com.javi.personal.tfg.processors.utils.{DataSourceProvider, DefaultDataSourceProvider}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

@ETL(table = ProcessedTables.PISOS_PROPERTIES)
class PisosProperties(
  config: ProcessorConfig,
  dataSourceProvider: DataSourceProvider = new DefaultDataSourceProvider(),
  customWriter: Option[SparkWriter] = None
)(implicit spark: SparkSession) extends Processor(config, dataSourceProvider, customWriter) {

  override protected val schema: StructType = StructType(Array(
      StructField(Id, StringType),
      StructField(Title, StringType),
      StructField(Price, IntegerType),
      StructField(Surface, IntegerType),
      StructField(Rooms, IntegerType),
      StructField(Bathrooms, IntegerType),
      StructField(Link, StringType),
      StructField(Source, StringType),
      StructField(CreationDate, DateType),
      StructField(Elevator, BooleanType),
      StructField(Garage, BooleanType),
      StructField(Garden, BooleanType),
      StructField(City, StringType),
      StructField(Country, StringType),
      StructField(PostalCode, IntegerType),
      StructField(Province, StringType),
      StructField(Region, StringType),
      StructField(ModificationDate, DateType),
      StructField(Operation, StringType),
      StructField(Pool, BooleanType),
      StructField(Description, StringType),
      StructField(Terrace, BooleanType),
      StructField(Type, StringType),
      StructField(Latitude, DoubleType),
      StructField(Longitude, DoubleType),
      StructField(ImageUrl, StringType)
    )
  )

  private object sources {
    lazy val sanitedPisosProperties: DataFrame = {
      val df = dataSourceProvider.readSilver("pisos", "properties", config.date)
      val withYear = if (df.columns.contains("year")) df else df.withColumn("year", lit(config.date.getYear))
      val withMonth = if (withYear.columns.contains("month")) withYear else withYear.withColumn("month", lit(config.date.getMonthValue))
      val withDay = if (withMonth.columns.contains("day")) withMonth else withMonth.withColumn("day", lit(config.date.getDayOfMonth))
      withDay
    }
    lazy val zipCodes: DataFrame = dataSourceProvider.readSilver("cnig", "zip_codes")
    lazy val sanitedProvinces: DataFrame = dataSourceProvider.readSilver("opendatasoft", "provincias-espanolas")
  }

  override protected def build(): DataFrame =
    PisosTransformer.transform(sources.sanitedPisosProperties, sources.zipCodes, sources.sanitedProvinces)

}

object PisosProperties {

  val Id = "id"
  val Title = "title"
  val Price = "price"
  val Surface = "surface"
  val Rooms = "rooms"
  val Bathrooms = "bathrooms"
  val Link = "link"
  val Source = "source"
  val CreationDate = "creation_date"
  val Elevator = "elevator"
  val Garage = "garage"
  val Garden = "garden"
  val City = "city"
  val Country = "country"
  val PostalCode = "postal_code"
  val Province = "province"
  val Region = "region"
  val ModificationDate = "modification_date"
  val Operation = "operation"
  val Pool = "pool"
  val Description = "description"
  val Terrace = "terrace"
  val Type = "type"
  val Latitude = "latitude"
  val LongType = "long_type"
  val Longitude = "longitude"
  val ImageUrl = "image_url"
}
