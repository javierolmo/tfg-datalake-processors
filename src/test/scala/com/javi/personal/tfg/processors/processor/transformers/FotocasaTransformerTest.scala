package com.javi.personal.tfg.processors.processor.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date

class FotocasaTransformerTest extends AnyFlatSpec with Matchers {

  private implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("FotocasaTransformerTest")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  it should "transform fotocasa silver data to properties_full schema correctly" in {
    val fotocasaInput = Seq(
      (
        2,
        0L,
        42.235,
        -8.719,
        "2026-09-27 14:42:20",
        4,
        190446371L,
        236,
        "Vigo",
        "comprar",
        750000,
        "pontevedra-provincia",
        "47 DAYS",
        "Flat",
        "viviendas",
        "Centro - Areal, Vigo",
        "https://www.fotocasa.es/item/190446371"
      )
    ).toDF(
      "baños",
      "coordenadas__accuracy",
      "coordenadas__latitude",
      "coordenadas__longitude",
      "fecha_scraping",
      "habitaciones",
      "id",
      "metros",
      "municipio",
      "operacion",
      "precio",
      "provincia",
      "publicado_hace",
      "tipo_detalle",
      "tipo_inmueble",
      "ubicacion",
      "url"
    )

    val result = FotocasaTransformer.transform(fotocasaInput)

    result.count() shouldEqual 1

    val row = result.first()
    row.getAs[String]("id") shouldEqual "190446371"
    row.getAs[Int]("price") shouldEqual 750000
    row.getAs[Int]("surface") shouldEqual 236
    row.getAs[Int]("rooms") shouldEqual 4
    row.getAs[Int]("bathrooms") shouldEqual 2
    row.getAs[String]("city") shouldEqual "Vigo"
    row.getAs[String]("province") shouldEqual "Pontevedra"
    row.getAs[String]("country") shouldEqual "ES"
    row.getAs[String]("source") shouldEqual "fotocasa"
    row.getAs[String]("link") shouldEqual "https://www.fotocasa.es/item/190446371"
    row.getAs[String]("operation") shouldEqual "SELL"
    row.getAs[String]("type") shouldEqual "FLAT"
    row.getAs[Double]("latitude") shouldEqual 42.235
    row.getAs[Double]("longitude") shouldEqual -8.719
    row.getAs[Date]("modification_date") shouldEqual Date.valueOf("2026-09-27")
  }

  it should "deduplicate by id keeping latest modification_date" in {
    val fotocasaInput = Seq(
      (2, 0L, 42.0, -8.0, "2026-09-26 10:00:00", 3, 100L, 80, "Vigo", "comprar", 200000, "pontevedra-provincia", "10 DAYS", "Flat", "viviendas", "Loc", "https://url1"),
      (2, 0L, 42.0, -8.0, "2026-09-27 10:00:00", 3, 100L, 80, "Vigo", "comprar", 190000, "pontevedra-provincia", "9 DAYS", "Flat", "viviendas", "Loc", "https://url2")
    ).toDF(
      "baños", "coordenadas__accuracy", "coordenadas__latitude", "coordenadas__longitude",
      "fecha_scraping", "habitaciones", "id", "metros", "municipio", "operacion",
      "precio", "provincia", "publicado_hace", "tipo_detalle", "tipo_inmueble", "ubicacion", "url"
    )

    val result = FotocasaTransformer.transform(fotocasaInput)
    result.count() shouldEqual 1
    val row = result.first()
    row.getAs[Int]("price") shouldEqual 190000
    row.getAs[Date]("modification_date") shouldEqual Date.valueOf("2026-09-27")
  }

  it should "transform using FotocasaSources case class input" in {
    val fotocasaInput = Seq(
      (1, 0L, 42.0, -8.0, "2026-09-27 10:00:00", 1, 200L, 50, "Vigo", "comprar", 100000, "pontevedra-provincia", "1 DAY", "Flat", "viviendas", "Loc", "https://url")
    ).toDF(
      "baños", "coordenadas__accuracy", "coordenadas__latitude", "coordenadas__longitude",
      "fecha_scraping", "habitaciones", "id", "metros", "municipio", "operacion",
      "precio", "provincia", "publicado_hace", "tipo_detalle", "tipo_inmueble", "ubicacion", "url"
    )

    val result = FotocasaTransformer.transform(FotocasaSources(fotocasaInput))
    result.count() shouldEqual 1
    result.first().getAs[String]("id") shouldEqual "200"
  }

  it should "map operation and type correctly to match standard values" in {
    val fotocasaInput = Seq(
      (1, 0L, 42.0, -8.0, "2026-09-27 10:00:00", 1, 101L, 50, "Vigo", "alquiler", 800, "pontevedra-provincia", "1 DAY", "Flat", "viviendas", "Loc", "https://url1"),
      (1, 0L, 42.0, -8.0, "2026-09-27 10:00:00", 1, 102L, 100, "Vigo", "comprar", 200000, "pontevedra-provincia", "1 DAY", "Business", "locales", "Loc", "https://url2"),
      (1, 0L, 42.0, -8.0, "2026-09-27 10:00:00", 1, 103L, 500, "Vigo", "comprar", 500000, "pontevedra-provincia", "1 DAY", "Building", "edificios", "Loc", "https://url3"),
      (1, 0L, 42.0, -8.0, "2026-09-27 10:00:00", 1, 104L, 200, "Vigo", "comprar", 300000, "pontevedra-provincia", "1 DAY", "House", "casas", "Loc", "https://url4")
    ).toDF(
      "baños", "coordenadas__accuracy", "coordenadas__latitude", "coordenadas__longitude",
      "fecha_scraping", "habitaciones", "id", "metros", "municipio", "operacion",
      "precio", "provincia", "publicado_hace", "tipo_detalle", "tipo_inmueble", "ubicacion", "url"
    )

    val result = FotocasaTransformer.transform(fotocasaInput)
    val map = result.collect().map(r => (r.getAs[String]("id"), (r.getAs[String]("operation"), r.getAs[String]("type")))).toMap

    map("101") shouldEqual ("RENT", "FLAT")
    map("102") shouldEqual ("SELL", "OFFICE")
    map("103") shouldEqual ("SELL", "OFFICE")
    map("104") shouldEqual ("SELL", "HOUSE")
  }

  it should "resolve postal_code, standardize City using zipCodes and enrich Region and Province with opendatasoft" in {
    val fotocasaInput = Seq(
      (1, 0L, 42.235, -8.719, "2026-09-27 10:00:00", 1, 999L, 50, "Vigo Raw", "comprar", 100000, "pontevedra-provincia", "1 DAY", "Flat", "viviendas", "Loc", "https://url", 2026, 9, 30)
    ).toDF(
      "baños", "coordenadas__accuracy", "coordenadas__latitude", "coordenadas__longitude",
      "fecha_scraping", "habitaciones", "id", "metros", "municipio", "operacion",
      "precio", "provincia", "publicado_hace", "tipo_detalle", "tipo_inmueble", "ubicacion", "url",
      "year", "month", "day"
    )

    val zipCodesSchema = new StructType()
      .add("codigo_postal", IntegerType)
      .add("nombre", StringType)
      .add("provincia", StringType)
      .add("coordinates", ArrayType(
        new StructType()
          .add("latitude", DoubleType)
          .add("longitude", DoubleType)
      ))

    val zipCodesData = Seq(
      Row(
        36201,
        "Vigo CNIG",
        "Pontevedra CNIG",
        Seq(
          Row(42.230, -8.725),
          Row(42.240, -8.725),
          Row(42.240, -8.710),
          Row(42.230, -8.710),
          Row(42.230, -8.725)
        )
      )
    )

    val zipCodes = spark.createDataFrame(
      spark.sparkContext.parallelize(zipCodesData),
      zipCodesSchema
    )

    val provinces = Seq((36, "Pontevedra", "Galicia")).toDF("codigo", "provincia", "ccaa")

    val result = FotocasaTransformer.transform(fotocasaInput, zipCodes, provinces)

    result.count() shouldEqual 1
    val row = result.first()
    row.getAs[Int]("postal_code") shouldEqual 36201
    row.getAs[String]("city") shouldEqual "Vigo CNIG"
    row.getAs[String]("province") shouldEqual "Pontevedra"
    row.getAs[String]("region") shouldEqual "Galicia"
    row.getAs[Date]("creation_date") shouldEqual Date.valueOf("2026-09-30")
  }

  it should "fallback modification_date to scrap date when fecha_scraping is null" in {
    val fotocasaInput = Seq(
      (1, 0L, 42.0, -8.0, null, 1, 888L, 50, "Vigo", "comprar", 100000, "pontevedra-provincia", "1 DAY", "Flat", "viviendas", "Loc", "https://url", 2026, 9, 30)
    ).toDF(
      "baños", "coordenadas__accuracy", "coordenadas__latitude", "coordenadas__longitude",
      "fecha_scraping", "habitaciones", "id", "metros", "municipio", "operacion",
      "precio", "provincia", "publicado_hace", "tipo_detalle", "tipo_inmueble", "ubicacion", "url",
      "year", "month", "day"
    )

    val result = FotocasaTransformer.transform(fotocasaInput)
    val row = result.first()
    row.getAs[Date]("creation_date") shouldEqual Date.valueOf("2026-09-30")
    row.getAs[Date]("modification_date") shouldEqual Date.valueOf("2026-09-30")
  }

  it should "map ascensor as elevator, parking as garage, and imagen_portada as image_url" in {
    val fotocasaInput = Seq(
      (1, 0L, 42.0, -8.0, "2026-09-27 10:00:00", 1, 777L, 50, "Vigo", "comprar", 100000, "pontevedra-provincia", "1 DAY", "Flat", "viviendas", "Loc", "https://url", "https://img.fotocasa.es/pic.jpg", true, true)
    ).toDF(
      "baños", "coordenadas__accuracy", "coordenadas__latitude", "coordenadas__longitude",
      "fecha_scraping", "habitaciones", "id", "metros", "municipio", "operacion",
      "precio", "provincia", "publicado_hace", "tipo_detalle", "tipo_inmueble", "ubicacion", "url",
      "imagen_portada", "ascensor", "parking"
    )

    val result = FotocasaTransformer.transform(fotocasaInput)
    val row = result.first()
    row.getAs[Boolean]("elevator") shouldEqual true
    row.getAs[Boolean]("garage") shouldEqual true
    row.getAs[String]("image_url") shouldEqual "https://img.fotocasa.es/pic.jpg"
  }

}
