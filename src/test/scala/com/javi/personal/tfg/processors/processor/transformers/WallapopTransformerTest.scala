package com.javi.personal.tfg.processors.processor.transformers

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date

class WallapopTransformerTest extends AnyFlatSpec with Matchers {

  private implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("WallapopTransformerTest")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  it should "transform wallapop data, enrich with provinces and generate link correctly" in {
    val wallapopInput = Seq(
      (
        "item-1",
        "Piso bonito",
        150000,
        80,
        3,
        1,
        "Madrid",
        "ES",
        28001,
        "Comunidad de Madrid",
        "sale",
        "flat",
        "Buen estado",
        "2024-01-02",
        "piso-bonito-1",
        "2024-01-01",
        40.4168,
        -3.7038
      )
    ).toDF(
      "id",
      "title",
      "price__amount",
      "type_attributes__surface",
      "type_attributes__rooms",
      "type_attributes__bathrooms",
      "location__city",
      "location__country_code",
      "location__postal_code",
      "location__region",
      "type_attributes__operation",
      "type_attributes__type",
      "description",
      "modified_at",
      "web_slug",
      "created_at",
      "location__latitude",
      "location__longitude"
    )

    val provincesInput = Seq(
      (28, "Madrid")
    ).toDF("codigo", "provincia")

    val result = WallapopTransformer.transform(wallapopInput, provincesInput)

    result.count() shouldEqual 1

    val row = result.first()
    row.getAs[String]("id") shouldEqual "item-1"
    row.getAs[String]("title") shouldEqual "Piso bonito"
    row.getAs[Int]("price") shouldEqual 150000
    row.getAs[String]("province") shouldEqual "Madrid"
    row.getAs[String]("source") shouldEqual "wallapop"
    row.getAs[String]("link") shouldEqual "https://es.wallapop.com/item/piso-bonito-1"
    row.getAs[String]("type") shouldEqual "FLAT"
    row.getAs[String]("operation") shouldEqual "SELL"
  }

  it should "standardize wallapop types such as Premises / Office and Box Room" in {
    val wallapopInput = Seq(
      ("w-1", "Local", 100000, 50, 0, 1, "Madrid", "ES", 28001, "Madrid", "Sell", "Premises / Office", "Desc", "2024-01-01", "slug-1", "2024-01-01", 40.0, -3.0),
      ("w-2", "Trastero", 10000, 10, 0, 0, "Madrid", "ES", 28001, "Madrid", "Rent", "Box Room", "Desc", "2024-01-01", "slug-2", "2024-01-01", 40.0, -3.0),
      ("w-3", "Chalet", 300000, 200, 4, 2, "Madrid", "ES", 28001, "Madrid", "sale", "House", "Desc", "2024-01-01", "slug-3", "2024-01-01", 40.0, -3.0)
    ).toDF(
      "id", "title", "price__amount", "type_attributes__surface", "type_attributes__rooms", "type_attributes__bathrooms",
      "location__city", "location__country_code", "location__postal_code", "location__region", "type_attributes__operation",
      "type_attributes__type", "description", "modified_at", "web_slug", "created_at", "location__latitude", "location__longitude"
    )

    val provincesInput = Seq((28, "Madrid")).toDF("codigo", "provincia")

    val result = WallapopTransformer.transform(wallapopInput, provincesInput)
    val map = result.collect().map(r => (r.getAs[String]("id"), (r.getAs[String]("operation"), r.getAs[String]("type")))).toMap

    map("w-1") shouldEqual ("SELL", "OFFICE")
    map("w-2") shouldEqual ("RENT", "BOXROOM")
    map("w-3") shouldEqual ("SELL", "HOUSE")
  }

  it should "map elevator, garage/parking, and images as image_url" in {
    val wallapopInput = Seq(
      ("w-feat", "Piso", 120000, 60, 2, 1, "Madrid", "ES", 28001, "Madrid", "sale", "flat", "Desc", "2024-01-01", "slug-feat", "2024-01-01", 40.0, -3.0, true, true, "https://cdn.wallapop.com/item.jpg")
    ).toDF(
      "id", "title", "price__amount", "type_attributes__surface", "type_attributes__rooms", "type_attributes__bathrooms",
      "location__city", "location__country_code", "location__postal_code", "location__region", "type_attributes__operation",
      "type_attributes__type", "description", "modified_at", "web_slug", "created_at", "location__latitude", "location__longitude",
      "elevator", "parking", "images"
    )

    val provincesInput = Seq((28, "Madrid")).toDF("codigo", "provincia")

    val result = WallapopTransformer.transform(wallapopInput, provincesInput)
    val row = result.first()
    row.getAs[Boolean]("elevator") shouldEqual true
    row.getAs[Boolean]("garage") shouldEqual true
    row.getAs[String]("image_url") shouldEqual "https://cdn.wallapop.com/item.jpg"
  }
}
