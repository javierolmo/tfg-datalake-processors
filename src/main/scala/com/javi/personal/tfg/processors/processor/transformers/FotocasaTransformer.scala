package com.javi.personal.tfg.processors.processor.transformers

import com.javi.personal.tfg.processors.processor.etls.FotocasaProperties._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, IntegerType, StringType}
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

case class FotocasaSources(sanitedFotocasa: DataFrame, zipCodes: DataFrame = null, provinces: DataFrame = null)

object FotocasaSources {
  def apply(sanitedFotocasa: DataFrame): FotocasaSources =
    FotocasaSources(sanitedFotocasa, null, null)

  def apply(sanitedFotocasa: DataFrame, zipCodes: DataFrame): FotocasaSources =
    FotocasaSources(sanitedFotocasa, zipCodes, null)
}

object FotocasaTransformer extends Transformer[FotocasaSources, DataFrame] {

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

  override def transform(sources: FotocasaSources): DataFrame =
    transform(sources.sanitedFotocasa, sources.zipCodes, sources.provinces)

  def transform(sanitedFotocasa: DataFrame): DataFrame =
    transform(sanitedFotocasa, null, null)

  def transform(sanitedFotocasa: DataFrame, zipCodes: DataFrame): DataFrame =
    transform(sanitedFotocasa, zipCodes, null)

  def transform(sanitedFotocasa: DataFrame, zipCodes: DataFrame, provinces: DataFrame): DataFrame = {
    val scrapDate = if (sanitedFotocasa.columns.contains("year") && sanitedFotocasa.columns.contains("month") && sanitedFotocasa.columns.contains("day")) {
      make_date(col("year").cast(IntegerType), col("month").cast(IntegerType), col("day").cast(IntegerType))
    } else if (sanitedFotocasa.columns.contains("fecha_scraping")) {
      to_date(col("fecha_scraping"))
    } else {
      lit(null).cast(DateType)
    }

    val mappedOperation = coalesce(
      OperationStandardizer.standardize(col("operacion")),
      OperationStandardizer.fromUrl(col("url"))
    )

    val mappedType = coalesce(
      PropertyTypeStandardizer.standardize(coalesce(col("tipo_detalle"), col("tipo_inmueble"))),
      PropertyTypeStandardizer.fromUrl(col("url"))
    )

    val elevatorCol = if (sanitedFotocasa.columns.contains("ascensor")) col("ascensor")
                      else if (sanitedFotocasa.columns.contains("elevator")) col("elevator")
                      else lit(null).cast(BooleanType)
    val garageCol = if (sanitedFotocasa.columns.contains("parking")) col("parking")
                    else if (sanitedFotocasa.columns.contains("garage")) col("garage")
                    else lit(null).cast(BooleanType)
    val imageUrlCol = if (sanitedFotocasa.columns.contains("imagen_portada")) {
      when(trim(col("imagen_portada")) =!= lit(""), col("imagen_portada")).otherwise(lit(null).cast(StringType))
    } else if (sanitedFotocasa.columns.contains("image_url")) {
      when(trim(col("image_url")) =!= lit(""), col("image_url")).otherwise(lit(null).cast(StringType))
    } else {
      lit(null).cast(StringType)
    }

    val base = sanitedFotocasa
      .withColumn(Id, col("id").cast(StringType))
      .withColumn(Title, lit(null).cast(StringType))
      .withColumn(Price, col("precio").cast(IntegerType))
      .withColumn(Surface, col("metros").cast(IntegerType))
      .withColumn(Rooms, col("habitaciones").cast(IntegerType))
      .withColumn(Bathrooms, col("baños").cast(IntegerType))
      .withColumn(Link, col("url"))
      .withColumn(Source, lit("fotocasa"))
      .withColumn(CreationDate, coalesce(lit(null).cast(DateType), scrapDate))
      .withColumn(Elevator, elevatorCol)
      .withColumn(Garage, garageCol)
      .withColumn(ImageUrl, imageUrlCol)
      .withColumn(Garden, lit(null).cast(BooleanType))
      .withColumn("raw_municipio", col("municipio"))
      .withColumn(Country, lit("ES"))
      .withColumn("raw_provincia_clean", initcap(regexp_replace(regexp_replace(col("provincia"), "-provincia$", ""), "-", " ")))
      .withColumn(ModificationDate, coalesce(to_date(col("fecha_scraping")), scrapDate))
      .withColumn(Operation, mappedOperation)
      .withColumn(Pool, lit(null).cast(BooleanType))
      .withColumn(Description, lit(null).cast(StringType))
      .withColumn(Terrace, lit(null).cast(BooleanType))
      .withColumn(Type, mappedType)
      .withColumn(Latitude, col("coordenadas__latitude").cast(DoubleType))
      .withColumn(Longitude, col("coordenadas__longitude").cast(DoubleType))

    val withZipCodes = if (zipCodes != null && !zipCodes.columns.isEmpty && zipCodes.columns.contains("coordinates") && zipCodes.columns.contains("codigo_postal")) {
      val zipCols = if (zipCodes.columns.contains("nombre")) Seq("codigo_postal", "coordinates", "nombre") else Seq("codigo_postal", "coordinates")
      val zipCodesWithPolygon = broadcast(
        zipCodes
          .select(zipCols.map(col): _*)
          .withColumn("coordinates", expr("transform(coordinates, x -> map_from_arrays(array('latitude', 'longitude'), array(x.latitude, x.longitude)))"))
      )

      val joined = base.join(zipCodesWithPolygon.as("z"), pointInPolygon(col(Latitude), col(Longitude), col("z.coordinates")) === lit(true), "left")
        .withColumn(PostalCode, col("z.codigo_postal").cast(IntegerType))

      if (zipCodes.columns.contains("nombre")) {
        joined.withColumn(City, coalesce(col("z.nombre"), col("raw_municipio")))
      } else {
        joined.withColumn(City, col("raw_municipio"))
      }
    } else {
      base
        .withColumn(PostalCode, lit(null).cast(IntegerType))
        .withColumn(City, col("raw_municipio"))
    }

    val withProvinces = if (provinces != null && !provinces.columns.isEmpty && provinces.columns.contains("codigo")) {
      val provCols = provinces.columns
      val selectCols = Seq("codigo") ++ (if (provCols.contains("provincia")) Seq("provincia") else Seq.empty) ++ (if (provCols.contains("ccaa")) Seq("ccaa") else Seq.empty)
      val provSubset = provinces.select(selectCols.map(c => col(c).as(s"prov_$c")): _*)

      val joined = withZipCodes
        .withColumn("province_code", (col(PostalCode) / 1000).cast(IntegerType))
        .join(provSubset, col("province_code") === col("prov_codigo").cast(IntegerType), "left")

      val withProv = if (provCols.contains("provincia")) {
        joined.withColumn(Province, coalesce(col("prov_provincia"), col("raw_provincia_clean")))
      } else {
        joined.withColumn(Province, col("raw_provincia_clean"))
      }

      val withReg = if (provCols.contains("ccaa")) {
        withProv.withColumn(Region, col("prov_ccaa"))
      } else {
        withProv.withColumn(Region, lit(null).cast(StringType))
      }

      withReg.drop(selectCols.map(c => s"prov_$c"): _*).drop("province_code", "raw_provincia_clean", "raw_municipio")
    } else {
      withZipCodes
        .withColumn(Province, col("raw_provincia_clean"))
        .withColumn(Region, lit(null).cast(StringType))
        .drop("raw_provincia_clean", "raw_municipio")
    }

    withProvinces
      .withColumn("row_number", row_number().over(Window.partitionBy(Id).orderBy(col(ModificationDate).desc)))
      .filter(col("row_number") === 1)
      .drop("row_number")
  }

}
