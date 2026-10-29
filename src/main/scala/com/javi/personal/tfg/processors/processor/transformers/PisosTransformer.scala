package com.javi.personal.tfg.processors.processor.transformers

import com.javi.personal.tfg.processors.processor.etls.PisosProperties._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DateType, IntegerType, StringType}
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

case class PisosSources(sanitedPisos: DataFrame, zipCodes: DataFrame, provinces: DataFrame = null)

object PisosSources {
  def apply(sanitedPisos: DataFrame, zipCodes: DataFrame): PisosSources =
    PisosSources(sanitedPisos, zipCodes, null)
}

object PisosTransformer extends Transformer[PisosSources, DataFrame] {

  val pointInPolygon = udf((lat: java.lang.Double, lon: java.lang.Double, coordinates: Seq[Map[String, Double]]) => {
    if (lat == null || lon == null || coordinates == null || coordinates.length < 3) {
      false
    } else {
      try {
        val coords: Array[Coordinate] = coordinates.map(coord => new Coordinate(coord("longitude"), coord("latitude"))).toArray
        val closedCoords = if (coords.nonEmpty && coords.head.equals2D(coords.last)) coords else coords :+ coords.head
        val gf = new GeometryFactory()
        val polygon = gf.createPolygon(closedCoords)
        val point = gf.createPoint(new Coordinate(lon, lat))
        polygon.contains(point)
      } catch {
        case _: Throwable => false
      }
    }
  })

  override def transform(sources: PisosSources): DataFrame =
    transform(sources.sanitedPisos, sources.zipCodes, sources.provinces)

  def transform(sanitedPisos: DataFrame, zipCodes: DataFrame): DataFrame =
    transform(sanitedPisos, zipCodes, null)

  def transform(sanitedPisos: DataFrame, zipCodes: DataFrame, provinces: DataFrame): DataFrame = {
    val scrapDate = if (sanitedPisos.columns.contains("year") && sanitedPisos.columns.contains("month") && sanitedPisos.columns.contains("day")) {
      make_date(col("year").cast(IntegerType), col("month").cast(IntegerType), col("day").cast(IntegerType))
    } else {
      lit(null).cast(DateType)
    }

    val mappedOperation = OperationStandardizer.fromUrl(col("url"))

    val mappedType = coalesce(
      PropertyTypeStandardizer.standardize(col("propertyType")),
      PropertyTypeStandardizer.fromUrl(col("url"))
    )

    val elevatorCol = if (sanitedPisos.columns.contains("elevator")) col("elevator")
                      else lit(null).cast(BooleanType)
    val garageCol = if (sanitedPisos.columns.contains("parking")) col("parking")
                    else if (sanitedPisos.columns.contains("garage")) col("garage")
                    else lit(null).cast(BooleanType)
    val imageUrlCol = if (sanitedPisos.columns.contains("imageUrl")) {
      when(trim(col("imageUrl")) =!= lit(""), col("imageUrl")).otherwise(lit(null).cast(StringType))
    } else if (sanitedPisos.columns.contains("image_url")) {
      when(trim(col("image_url")) =!= lit(""), col("image_url")).otherwise(lit(null).cast(StringType))
    } else {
      lit(null).cast(StringType)
    }

    val pisosRenamed = sanitedPisos
      .withColumn(Type, mappedType)
      .drop("propertyType")
      .withColumn(Operation, mappedOperation)
      .withColumnRenamed("id", Id)
      .withColumnRenamed("title", Title)
      .withColumnRenamed("price", Price)
      .withColumnRenamed("surface", Surface)
      .withColumnRenamed("rooms", Rooms)
      .withColumnRenamed("bathrooms", Bathrooms)
      .withColumnRenamed("url", Link)
      .withColumnRenamed("fullDescription", Description)
      .withColumnRenamed("latitude", Latitude)
      .withColumnRenamed("longitude", Longitude)

    val withZipCodes = if (zipCodes != null && !zipCodes.columns.isEmpty && zipCodes.columns.contains("coordinates")) {
      val zipCodesWithPolygon = zipCodes
        .withColumn("coordinates", expr("transform(coordinates, x -> map_from_arrays(array('latitude', 'longitude'), array(x.latitude, x.longitude)))"))

      pisosRenamed
        .join(zipCodesWithPolygon.as("z"), pointInPolygon(col(Latitude), col(Longitude), col("z.coordinates")) === lit(true), "left")
        .withColumn(City, col("z.nombre"))
        .withColumn(PostalCode, col("z.codigo_postal").cast(IntegerType))
        .withColumn("zip_provincia", col("z.provincia"))
    } else {
      pisosRenamed
        .withColumn(City, lit(null).cast(StringType))
        .withColumn(PostalCode, lit(null).cast(IntegerType))
        .withColumn("zip_provincia", lit(null).cast(StringType))
    }

    val withProvinces = if (provinces != null && !provinces.columns.isEmpty && provinces.columns.contains("codigo")) {
      val provCols = provinces.columns
      val selectCols = Seq("codigo") ++ (if (provCols.contains("provincia")) Seq("provincia") else Seq.empty) ++ (if (provCols.contains("ccaa")) Seq("ccaa") else Seq.empty)
      val provSubset = provinces.select(selectCols.map(c => col(c).as(s"prov_$c")): _*)

      val joined = withZipCodes
        .withColumn("province_code", (col(PostalCode) / 1000).cast(IntegerType))
        .join(provSubset, col("province_code") === col("prov_codigo").cast(IntegerType), "left")

      val withProv = if (provCols.contains("provincia")) {
        joined.withColumn(Province, coalesce(col("prov_provincia"), col("zip_provincia")))
      } else {
        joined.withColumn(Province, col("zip_provincia"))
      }

      val withReg = if (provCols.contains("ccaa")) {
        withProv.withColumn(Region, col("prov_ccaa"))
      } else {
        withProv.withColumn(Region, lit(null).cast(StringType))
      }

      withReg.drop(selectCols.map(c => s"prov_$c"): _*).drop("province_code", "zip_provincia")
    } else {
      withZipCodes
        .withColumn(Province, col("zip_provincia"))
        .withColumn(Region, lit(null).cast(StringType))
        .drop("zip_provincia")
    }

    withProvinces
      .withColumn(Source, lit("pisos.com"))
      .withColumn(CreationDate, coalesce(lit(null).cast(DateType), scrapDate))
      .withColumn(Elevator, elevatorCol)
      .withColumn(Garage, garageCol)
      .withColumn(ImageUrl, imageUrlCol)
      .withColumn(Garden, lit(null).cast(BooleanType))
      .withColumn(Country, lit("ES"))
      .withColumn(Pool, lit(null).cast(BooleanType))
      .withColumn(Terrace, lit(null).cast(BooleanType))
      .withColumn(ModificationDate, coalesce(to_date(col("lastUpdateDate")), scrapDate))
      .withColumn("row_number", row_number().over(Window.partitionBy(Id).orderBy(col(ModificationDate).desc)))
      .filter(col("row_number") === 1)
      .drop("row_number")
  }

}
