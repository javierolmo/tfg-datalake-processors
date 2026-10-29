package com.javi.personal.tfg.processors.cleaner

import com.javi.personal.tfg.processors.cleaner.model.MetadataCatalog
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CleanerTest extends AnyFlatSpec with Matchers {

  private implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("CleanerTest")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  private val catalog = MetadataCatalog.default()

  "Cleaner.validate for Fotocasa" should "clean non-numeric string 'No disponible' as null without casting error" in {
    val fotocasaMeta = catalog.findByCatalogItem("fotocasa_properties").get
    val rawDF = Seq(
      ("No disponible", "No disponible", "480", "190446371", "275.000 €", "2026-09-27 14:42:20")
    ).toDF("baños", "habitaciones", "metros", "id", "precio", "fecha_scraping")

    val result = Cleaner.validate(rawDF, fotocasaMeta)

    result.invalidRecords.count() shouldEqual 0
    result.validRecords.count() shouldEqual 1

    val validRow = result.validRecords.head()
    validRow.isNullAt(validRow.fieldIndex("baños")) shouldBe true
    validRow.isNullAt(validRow.fieldIndex("habitaciones")) shouldBe true
    validRow.getInt(validRow.fieldIndex("metros")) shouldEqual 480
    validRow.getInt(validRow.fieldIndex("precio")) shouldEqual 275000
  }

  it should "handle new fields imagen_portada, ascensor, and parking with parseBoolean logic" in {
    val fotocasaMeta = catalog.findByCatalogItem("fotocasa_properties").get
    val rawDF = Seq(
      ("101", "250.000 €", "2026-09-27 14:42:20", "https://img.fotocasa.es/1.jpg", "true", "uncastable_parking"),
      ("102", "150.000 €", "2026-09-27 14:42:20", null, null, "true")
    ).toDF("id", "precio", "fecha_scraping", "imagen_portada", "ascensor", "parking")

    val result = Cleaner.validate(rawDF, fotocasaMeta)

    result.invalidRecords.count() shouldEqual 0
    result.validRecords.count() shouldEqual 2

    val rows = result.validRecords.collect()
    val row1 = rows.find(_.getAs[Long]("id") == 101L).get
    row1.getAs[String]("imagen_portada") shouldEqual "https://img.fotocasa.es/1.jpg"
    row1.getAs[Boolean]("ascensor") shouldBe true
    row1.getAs[Boolean]("parking") shouldBe false // uncastable -> false

    val row2 = rows.find(_.getAs[Long]("id") == 102L).get
    row2.isNullAt(row2.fieldIndex("imagen_portada")) shouldBe true
    row2.getAs[Boolean]("ascensor") shouldBe false // null -> false
    row2.getAs[Boolean]("parking") shouldBe true
  }

  "Cleaner.validate for Wallapop" should "discard listings with international postal codes like '4900-809'" in {
    val wallapopMeta = catalog.findByCatalogItem("wallapop_properties_2").get
    val rawDF = Seq(
      (200, "2026-08-09T10:00:00.000Z", "Some description", "item-123", "4900-809", 150000.0)
    ).toDF("category_id", "created_at", "description", "id", "location__postal_code", "price__amount")

    val result = Cleaner.validate(rawDF, wallapopMeta)

    result.validRecords.count() shouldEqual 0
    result.invalidRecords.count() shouldEqual 1
  }

  it should "handle elevator, garage, and parking with alias fallback and default false on null or uncastable" in {
    val wallapopMeta = catalog.findByCatalogItem("wallapop_properties_2").get
    val rawDF = Seq(
      (200, "item-1", "title 1", "2026-08-09T10:00:00.000Z", 28001, "true", null, "uncastable_xyz"),
      (200, "item-2", "title 2", "2026-08-09T10:00:00.000Z", 28002, null, "true", "true")
    ).toDF("category_id", "id", "title", "created_at", "location__postal_code", "type_attributes__elevator", "type_attributes__garage", "parking")

    val result = Cleaner.validate(rawDF, wallapopMeta)

    result.invalidRecords.count() shouldEqual 0
    result.validRecords.count() shouldEqual 2

    val rows = result.validRecords.collect()
    val row1 = rows.find(_.getAs[String]("id") == "item-1").get
    row1.getAs[Boolean]("elevator") shouldBe true
    row1.getAs[Boolean]("garage") shouldBe false // null -> false
    row1.getAs[Boolean]("parking") shouldBe false // uncastable -> false

    val row2 = rows.find(_.getAs[String]("id") == "item-2").get
    row2.getAs[Boolean]("elevator") shouldBe false // null -> false
    row2.getAs[Boolean]("garage") shouldBe true
    row2.getAs[Boolean]("parking") shouldBe true
  }

  "Cleaner.validate for Pisos" should "parse listings when lastUpdateDate has array format '[2026,2,19]'" in {
    val pisosMeta = catalog.findByCatalogItem("pisos_properties").get
    val rawDF = Seq(
      ("p-123", "Piso en centro", "250.000 €", "[2026,2,19]", "3", "2")
    ).toDF("id", "title", "price", "lastUpdateDate", "rooms", "bathrooms")

    val result = Cleaner.validate(rawDF, pisosMeta)

    result.validRecords.count() shouldEqual 1
    result.invalidRecords.count() shouldEqual 0
    result.validRecords.first().getAs[java.sql.Date]("lastUpdateDate") shouldEqual java.sql.Date.valueOf("2026-02-19")
  }

  it should "keep listings when lastUpdateDate is a standard ISO date '2026-02-19'" in {
    val pisosMeta = catalog.findByCatalogItem("pisos_properties").get
    val rawDF = Seq(
      ("p-123", "Piso en centro", "250.000 €", "2026-02-19", "3", "2")
    ).toDF("id", "title", "price", "lastUpdateDate", "rooms", "bathrooms")

    val result = Cleaner.validate(rawDF, pisosMeta)

    result.validRecords.count() shouldEqual 1
    result.invalidRecords.count() shouldEqual 0
    result.validRecords.first().getAs[java.sql.Date]("lastUpdateDate") shouldEqual java.sql.Date.valueOf("2026-02-19")
  }

  it should "parse listings when lastUpdateDate is in Spanish format '19/02/2026'" in {
    val pisosMeta = catalog.findByCatalogItem("pisos_properties").get
    val rawDF = Seq(
      ("p-123", "Piso en centro", "250.000 €", "19/02/2026", "3", "2")
    ).toDF("id", "title", "price", "lastUpdateDate", "rooms", "bathrooms")

    val result = Cleaner.validate(rawDF, pisosMeta)

    result.validRecords.count() shouldEqual 1
    result.invalidRecords.count() shouldEqual 0
    result.validRecords.first().getAs[java.sql.Date]("lastUpdateDate") shouldEqual java.sql.Date.valueOf("2026-02-19")
  }

  it should "handle elevator and parking with default false on null or uncastable, and cast floor to integer" in {
    val pisosMeta = catalog.findByCatalogItem("pisos_properties").get
    val rawDF = Seq(
      ("p-1", "Piso con ascensor", "250.000 €", "2026-02-19", "3", "2", "true", "xyz_uncastable", "4"),
      ("p-2", "Piso sin ascensor", "180.000 €", "2026-02-19", "2", "1", null, null, "2º")
    ).toDF("id", "title", "price", "lastUpdateDate", "rooms", "bathrooms", "elevator", "parking", "floor")

    val result = Cleaner.validate(rawDF, pisosMeta)

    result.validRecords.count() shouldEqual 2
    result.invalidRecords.count() shouldEqual 0

    val rows = result.validRecords.collect()
    val row1 = rows.find(_.getAs[String]("id") == "p-1").get
    row1.getAs[Boolean]("elevator") shouldBe true
    row1.getAs[Boolean]("parking") shouldBe false
    row1.getAs[Int]("floor") shouldEqual 4

    val row2 = rows.find(_.getAs[String]("id") == "p-2").get
    row2.getAs[Boolean]("elevator") shouldBe false
    row2.getAs[Boolean]("parking") shouldBe false
    row2.getAs[Int]("floor") shouldEqual 2
  }

}
