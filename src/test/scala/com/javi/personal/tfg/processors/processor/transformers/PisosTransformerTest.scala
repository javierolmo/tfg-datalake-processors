package com.javi.personal.tfg.processors.processor.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date

class PisosTransformerTest extends AnyFlatSpec with Matchers {

  private implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("PisosTransformerTest")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  it should "map operation from url and type from propertyType" in {
    val pisosInput = Seq(
      (
        "p-1",
        "Piso céntrico",
        150000,
        "https://www.pisos.com/comprar/piso-vigo-1/",
        "Buen piso",
        3,
        1,
        80,
        1,
        "http://img.jpg",
        Date.valueOf("2026-09-27"),
        42.23,
        -8.72,
        "Piso",
        "Vigo"
      ),
      (
        "p-2",
        "Casa con jardín",
        250000,
        "https://www.pisos.com/alquilar/casa-baiona-2/",
        "Buena casa",
        4,
        2,
        200,
        0,
        "http://img.jpg",
        Date.valueOf("2026-09-27"),
        42.11,
        -8.84,
        "Casa",
        "Baiona"
      ),
      (
        "p-3",
        "Local comercial",
        300000,
        "https://www.pisos.com/comprar/local-vigo-3/",
        "Buen local",
        0,
        1,
        120,
        0,
        "http://img.jpg",
        Date.valueOf("2026-09-27"),
        42.24,
        -8.70,
        "Local",
        "Vigo"
      ),
      (
        "p-4",
        "Ático con terraza",
        180000,
        "https://www.pisos.com/comprar/atico-vigo-4/",
        "Ático luminoso",
        2,
        1,
        70,
        4,
        "http://img.jpg",
        Date.valueOf("2026-09-27"),
        42.24,
        -8.70,
        "Ático",
        "Vigo"
      ),
      (
        "p-5",
        "Dúplex moderno",
        220000,
        "https://www.pisos.com/comprar/duplex-vigo-5/",
        "Dúplex amplio",
        3,
        2,
        110,
        3,
        "http://img.jpg",
        Date.valueOf("2026-09-27"),
        42.24,
        -8.70,
        "Dúplex",
        "Vigo"
      ),
      (
        "p-6",
        "Estudio céntrico",
        90000,
        "https://www.pisos.com/alquilar/estudio-vigo-6/",
        "Estudio coqueto",
        1,
        1,
        40,
        2,
        "http://img.jpg",
        Date.valueOf("2026-09-27"),
        42.24,
        -8.70,
        "Estudio",
        "Vigo"
      ),
      (
        "p-7",
        "Loft diáfano",
        130000,
        "https://www.pisos.com/comprar/loft-vigo-7/",
        "Loft de diseño",
        1,
        1,
        60,
        1,
        "http://img.jpg",
        Date.valueOf("2026-09-27"),
        42.24,
        -8.70,
        "Loft",
        "Vigo"
      ),
      (
        "p-8",
        "Apartamento sin propertyType",
        110000,
        "https://www.pisos.com/alquilar/apartamento-berbes-8/",
        "Apartamento reformado",
        1,
        1,
        50,
        1,
        "http://img.jpg",
        Date.valueOf("2026-09-27"),
        42.24,
        -8.70,
        null,
        "Vigo"
      )
    ).toDF(
      "id",
      "title",
      "price",
      "url",
      "fullDescription",
      "rooms",
      "bathrooms",
      "surface",
      "floor",
      "imageUrl",
      "lastUpdateDate",
      "latitude",
      "longitude",
      "propertyType",
      "location"
    )

    val zipCodes = spark.createDataFrame(
      spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
      new org.apache.spark.sql.types.StructType()
        .add("coordinates", org.apache.spark.sql.types.ArrayType(
          new org.apache.spark.sql.types.StructType()
            .add("latitude", org.apache.spark.sql.types.DoubleType)
            .add("longitude", org.apache.spark.sql.types.DoubleType)
        ))
        .add("codigo_postal", org.apache.spark.sql.types.IntegerType)
        .add("nombre", org.apache.spark.sql.types.StringType)
        .add("provincia", org.apache.spark.sql.types.StringType)
    )

    val result = PisosTransformer.transform(pisosInput, zipCodes)

    result.count() shouldEqual 8

    val rows = result.collect().map(r => (r.getAs[String]("id"), (r.getAs[String]("operation"), r.getAs[String]("type")))).toMap

    rows("p-1") shouldEqual ("SELL", "FLAT")
    rows("p-2") shouldEqual ("RENT", "HOUSE")
    rows("p-3") shouldEqual ("SELL", "OFFICE")
    rows("p-4") shouldEqual ("SELL", "FLAT")
    rows("p-5") shouldEqual ("SELL", "FLAT")
    rows("p-6") shouldEqual ("RENT", "FLAT")
    rows("p-7") shouldEqual ("SELL", "FLAT")
    rows("p-8") shouldEqual ("RENT", "FLAT")
  }

  it should "enrich Region and Province from opendatasoft provincias and City from zipCodes" in {
    val pisosInput = Seq(
      ("p-loc-1", "Piso", 100000, "https://www.pisos.com/comprar/piso-vigo/", "Desc", 2, 1, 70, 1, "http://img.jpg", Date.valueOf("2026-09-27"), 42.235, -8.719, "Piso", "Vigo", 2026, 9, 30)
    ).toDF(
      "id", "title", "price", "url", "fullDescription", "rooms", "bathrooms", "surface", "floor", "imageUrl",
      "lastUpdateDate", "latitude", "longitude", "propertyType", "location", "year", "month", "day"
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

    val result = PisosTransformer.transform(pisosInput, zipCodes, provinces)
    val row = result.first()

    row.getAs[String]("city") shouldEqual "Vigo CNIG"
    row.getAs[String]("province") shouldEqual "Pontevedra"
    row.getAs[String]("region") shouldEqual "Galicia"
    row.getAs[Date]("creation_date") shouldEqual Date.valueOf("2026-09-30")
    row.getAs[Date]("modification_date") shouldEqual Date.valueOf("2026-09-27")
  }

  it should "fallback modification_date to scrap date when null" in {
    val pisosInput = Seq(
      ("p-null-date", "Piso", 100000, "https://www.pisos.com/comprar/piso-vigo/", "Desc", 2, 1, 70, 1, "http://img.jpg", null, 42.235, -8.719, "Piso", "Vigo", 2026, 9, 30)
    ).toDF(
      "id", "title", "price", "url", "fullDescription", "rooms", "bathrooms", "surface", "floor", "imageUrl",
      "lastUpdateDate", "latitude", "longitude", "propertyType", "location", "year", "month", "day"
    )

    val zipCodes = spark.emptyDataFrame
    val result = PisosTransformer.transform(pisosInput, zipCodes)
    val row = result.first()

    row.getAs[Date]("creation_date") shouldEqual Date.valueOf("2026-09-30")
    row.getAs[Date]("modification_date") shouldEqual Date.valueOf("2026-09-30")
  }

  it should "map elevator, parking as garage, and imageUrl as image_url" in {
    val pisosInput = Seq(
      ("p-features", "Piso", 100000, "https://www.pisos.com/comprar/piso-vigo/", "Desc", 2, 1, 70, 1, "https://fotos.imghs.net/1.jpg", Date.valueOf("2026-09-27"), 42.235, -8.719, "Piso", "Vigo", true, true)
    ).toDF(
      "id", "title", "price", "url", "fullDescription", "rooms", "bathrooms", "surface", "floor", "imageUrl",
      "lastUpdateDate", "latitude", "longitude", "propertyType", "location", "elevator", "parking"
    )

    val result = PisosTransformer.transform(pisosInput, spark.emptyDataFrame)
    val row = result.first()
    row.getAs[Boolean]("elevator") shouldEqual true
    row.getAs[Boolean]("garage") shouldEqual true
    row.getAs[String]("image_url") shouldEqual "https://fotos.imghs.net/1.jpg"
  }

}
