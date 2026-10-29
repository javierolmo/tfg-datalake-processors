package com.javi.personal.tfg.processors.processor.transformers

import com.javi.personal.tfg.processors.processor.etls.WallapopProperties._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DateType, IntegerType, LongType, StringType}

case class WallapopSources(sanitedWallapop: DataFrame, provinces: DataFrame, zipCodes: DataFrame = null)

object WallapopSources {
  def apply(sanitedWallapop: DataFrame, provinces: DataFrame): WallapopSources =
    WallapopSources(sanitedWallapop, provinces, null)
}

object WallapopTransformer extends Transformer[WallapopSources, DataFrame] {

  private def sanitizeDate(dateCol: Column, fallback: Column): Column = {
    val ts = to_timestamp(dateCol)
    val yr = year(ts)
    val recoveredDate = to_date(from_unixtime(ts.cast(LongType) / 1000))
    val normalDate = to_date(ts)
    when(dateCol.isNotNull && yr.between(1990, 2050), normalDate)
      .when(dateCol.isNotNull && yr > 2050 && year(recoveredDate).between(1990, 2050), recoveredDate)
      .otherwise(fallback)
  }

  override def transform(sources: WallapopSources): DataFrame =
    transform(sources.sanitedWallapop, sources.provinces, sources.zipCodes)

  def transform(sanitedWallapop: DataFrame, provinces: DataFrame): DataFrame =
    transform(sanitedWallapop, provinces, null)

  def transform(sanitedWallapop: DataFrame, provinces: DataFrame, zipCodes: DataFrame): DataFrame = {
    val scrapDate = if (sanitedWallapop.columns.contains("year") && sanitedWallapop.columns.contains("month") && sanitedWallapop.columns.contains("day")) {
      make_date(col("year").cast(IntegerType), col("month").cast(IntegerType), col("day").cast(IntegerType))
    } else {
      lit(null).cast(DateType)
    }

    val elevatorCol = if (sanitedWallapop.columns.contains("elevator")) col("elevator")
                      else if (sanitedWallapop.columns.contains("type_attributes__elevator")) col("type_attributes__elevator")
                      else lit(null).cast(BooleanType)
    val garageCol = if (sanitedWallapop.columns.contains("garage")) col("garage")
                    else if (sanitedWallapop.columns.contains("parking")) col("parking")
                    else if (sanitedWallapop.columns.contains("type_attributes__garage")) col("type_attributes__garage")
                    else if (sanitedWallapop.columns.contains("type_attributes__parking")) col("type_attributes__parking")
                    else lit(null).cast(BooleanType)
    val imageUrlCol = if (sanitedWallapop.columns.contains("images")) {
                        when(trim(col("images")) =!= lit(""), col("images")).otherwise(lit(null).cast(StringType))
                      } else if (sanitedWallapop.columns.contains("image_url")) {
                        when(trim(col("image_url")) =!= lit(""), col("image_url")).otherwise(lit(null).cast(StringType))
                      } else {
                        lit(null).cast(StringType)
                      }

    val withProvinceCode = sanitedWallapop
      .withColumn("province_code", (col("location__postal_code").cast(IntegerType) / 1000).cast(IntegerType))

    val withProvinces = if (provinces != null && !provinces.columns.isEmpty && provinces.columns.contains("codigo")) {
      val provCols = provinces.columns
      val selectCols = Seq("codigo") ++ (if (provCols.contains("provincia")) Seq("provincia") else Seq.empty) ++ (if (provCols.contains("ccaa")) Seq("ccaa") else Seq.empty)
      val provSubset = provinces.select(selectCols.map(c => col(c).as(s"prov_$c")): _*)

      val joined = withProvinceCode.join(provSubset, col("province_code") === col("prov_codigo").cast(IntegerType), "left")

      val withProv = if (provCols.contains("provincia")) {
        joined.withColumn(Province, col("prov_provincia"))
      } else {
        joined.withColumn(Province, lit(null).cast(StringType))
      }

      val withReg = if (provCols.contains("ccaa")) {
        withProv.withColumn(Region, coalesce(col("prov_ccaa"), col("location__region")))
      } else {
        withProv.withColumn(Region, col("location__region"))
      }

      withReg.drop(selectCols.map(c => s"prov_$c"): _*).drop("province_code")
    } else {
      withProvinceCode
        .withColumn(Province, lit(null).cast(StringType))
        .withColumn(Region, col("location__region"))
        .drop("province_code")
    }

    val withCity = if (zipCodes != null && !zipCodes.columns.isEmpty && zipCodes.columns.contains("codigo_postal") && zipCodes.columns.contains("nombre")) {
      val zipLookup = zipCodes
        .select(col("codigo_postal").cast(IntegerType).as("z_code"), col("nombre").as("z_city"))
        .dropDuplicates("z_code")
      withProvinces
        .join(zipLookup, col("location__postal_code").cast(IntegerType) === col("z_code"), "left")
        .withColumn(City, coalesce(col("z_city"), col("location__city")))
        .drop("z_code", "z_city")
    } else {
      withProvinces.withColumn(City, col("location__city"))
    }

    withCity
      .withColumnRenamed("id", Id)
      .withColumnRenamed("title", Title)
      .withColumn(Price, col("price__amount").cast(IntegerType))
      .drop("price__amount")
      .withColumnRenamed("type_attributes__surface", Surface)
      .withColumnRenamed("type_attributes__rooms", Rooms)
      .withColumnRenamed("type_attributes__bathrooms", Bathrooms)
      .withColumnRenamed("location__country_code", Country)
      .withColumnRenamed("location__postal_code", PostalCode)
      .withColumn(Operation, OperationStandardizer.standardize(col("type_attributes__operation")))
      .drop("type_attributes__operation")
      .withColumn(Type, PropertyTypeStandardizer.standardize(col("type_attributes__type")))
      .drop("type_attributes__type")
      .withColumnRenamed("description", Description)
      .withColumn(ModificationDate, sanitizeDate(col("modified_at"), scrapDate))
      .withColumn(Source, lit("wallapop"))
      .withColumn(Link, concat(lit("https://es.wallapop.com/item/"), col("web_slug")))
      .withColumn(CreationDate, sanitizeDate(col("created_at"), scrapDate))
      .withColumn(Elevator, elevatorCol)
      .withColumn(Garage, garageCol)
      .withColumn(ImageUrl, imageUrlCol)
      .withColumn(Garden, lit(null).cast(BooleanType))
      .withColumn(Pool, lit(null).cast(BooleanType))
      .withColumn(Terrace, lit(null).cast(BooleanType))
      .withColumnRenamed("location__latitude", Latitude)
      .withColumnRenamed("location__longitude", Longitude)
      .drop("location__region", "location__city")
      .dropDuplicates(Title, Price, Description, Surface, Operation)
  }

}
