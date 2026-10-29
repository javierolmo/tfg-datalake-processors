package com.javi.personal.tfg.processors.processor.transformers

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date

class PropertiesFullTransformerTest extends AnyFlatSpec with Matchers {

  private implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("PropertiesFullTransformerTest")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  it should "deduplicate latest record by modification_date for each dataset and union them" in {
    val wallapopDf = Seq(
      ("w-1", "Walla Antiguo", Date.valueOf("2024-01-01"), 2024, 1, 1),
      ("w-1", "Walla Nuevo", Date.valueOf("2024-01-05"), 2024, 1, 5)
    ).toDF("id", "title", "modification_date", "year", "month", "day")

    val pisosDf = Seq(
      ("p-1", "Pisos Antiguo", Date.valueOf("2024-01-02"), 2024, 1, 2),
      ("p-1", "Pisos Nuevo", Date.valueOf("2024-01-10"), 2024, 1, 10),
      ("p-2", "Pisos Unico", Date.valueOf("2024-01-03"), 2024, 1, 3)
    ).toDF("id", "title", "modification_date", "year", "month", "day")

    val result = PropertiesFullTransformer.transform(wallapopDf, pisosDf)

    result.count() shouldEqual 3

    val rows = result.collect().map(r => (r.getAs[String]("id"), r.getAs[String]("title"))).toMap
    rows("w-1") shouldEqual "Walla Nuevo"
    rows("p-1") shouldEqual "Pisos Nuevo"
    rows("p-2") shouldEqual "Pisos Unico"
  }

  it should "transform using PropertiesFullSources case class input" in {
    val wallapopDf = Seq(("w-1", "Walla", Date.valueOf("2024-01-01"), 2024, 1, 1)).toDF("id", "title", "modification_date", "year", "month", "day")
    val pisosDf = Seq(("p-1", "Pisos", Date.valueOf("2024-01-01"), 2024, 1, 1)).toDF("id", "title", "modification_date", "year", "month", "day")

    val result = PropertiesFullTransformer.transform(PropertiesFullSources(wallapopDf, pisosDf))
    result.count() shouldEqual 2
  }

  it should "deduplicate latest record by modification_date for wallapop, pisos and fotocasa and union all three" in {
    val wallapopDf = Seq(
      ("w-1", "Walla Antiguo", Date.valueOf("2024-01-01"), 2024, 1, 1),
      ("w-1", "Walla Nuevo", Date.valueOf("2024-01-05"), 2024, 1, 5)
    ).toDF("id", "title", "modification_date", "year", "month", "day")

    val pisosDf = Seq(
      ("p-1", "Pisos Antiguo", Date.valueOf("2024-01-02"), 2024, 1, 2),
      ("p-1", "Pisos Nuevo", Date.valueOf("2024-01-10"), 2024, 1, 10)
    ).toDF("id", "title", "modification_date", "year", "month", "day")

    val fotocasaDf = Seq(
      ("f-1", "Foto Antiguo", Date.valueOf("2024-01-03"), 2024, 1, 3),
      ("f-1", "Foto Nuevo", Date.valueOf("2024-01-15"), 2024, 1, 15),
      ("f-2", "Foto Unico", Date.valueOf("2024-01-04"), 2024, 1, 4)
    ).toDF("id", "title", "modification_date", "year", "month", "day")

    val result = PropertiesFullTransformer.transform(wallapopDf, pisosDf, fotocasaDf)

    result.count() shouldEqual 4

    val rows = result.collect().map(r => (r.getAs[String]("id"), r.getAs[String]("title"))).toMap
    rows("w-1") shouldEqual "Walla Nuevo"
    rows("p-1") shouldEqual "Pisos Nuevo"
    rows("f-1") shouldEqual "Foto Nuevo"
    rows("f-2") shouldEqual "Foto Unico"
  }

  it should "calculate load_date from year, month, and day fields of its sources" in {
    val wallapopDf = Seq(
      ("w-1", "Walla", Date.valueOf("2024-01-01"), 2026, 2, 13)
    ).toDF("id", "title", "modification_date", "year", "month", "day")

    val pisosDf = Seq(
      ("p-1", "Pisos", Date.valueOf("2024-01-02"), "2026", "09", "27")
    ).toDF("id", "title", "modification_date", "year", "month", "day")

    val fotocasaDf = Seq(
      ("f-1", "Foto", Date.valueOf("2024-01-03"), 2026, 9, 27)
    ).toDF("id", "title", "modification_date", "year", "month", "day")

    val result = PropertiesFullTransformer.transform(wallapopDf, pisosDf, fotocasaDf)

    result.count() shouldEqual 3
    val rows = result.collect().map(r => (r.getAs[String]("id"), r.getAs[Date]("load_date"))).toMap
    rows("w-1") shouldEqual Date.valueOf("2026-02-13")
    rows("p-1") shouldEqual Date.valueOf("2026-09-27")
    rows("f-1") shouldEqual Date.valueOf("2026-09-27")
  }

  it should "sanitize corrupt dates exceeding year 2050 to recovered date or fallback" in {
    val wallapopDf = Seq(
      ("w-corrupt", "Walla", "+58011-09-30", "+58011-09-29", 2026, 2, 13)
    ).toDF("id", "title", "modification_date", "creation_date", "year", "month", "day")

    val pisosDf = Seq(
      ("p-1", "Pisos", "2026-02-18", "2026-02-18", 2026, 2, 18)
    ).toDF("id", "title", "modification_date", "creation_date", "year", "month", "day")

    val result = PropertiesFullTransformer.transform(wallapopDf, pisosDf)
    val wallaRow = result.filter($"id" === "w-corrupt").first()
    wallaRow.getAs[Date]("modification_date") shouldEqual Date.valueOf("2026-01-15")
    wallaRow.getAs[Date]("creation_date") shouldEqual Date.valueOf("2026-01-15")
  }

  it should "fail if year, month, or day is missing from a source" in {
    val invalidDf = Seq(("w-1", "Walla", Date.valueOf("2024-01-01"))).toDF("id", "title", "modification_date")
    val validDf = Seq(("p-1", "Pisos", Date.valueOf("2024-01-01"), 2024, 1, 1)).toDF("id", "title", "modification_date", "year", "month", "day")

    assertThrows[AnalysisException] {
      PropertiesFullTransformer.transform(invalidDf, validDf)
    }
  }

  it should "propagate image_url from all sources to properties_full" in {
    val wallapopDf = Seq(
      ("w-1", "Walla", Date.valueOf("2024-01-01"), 2024, 1, 1, "https://cdn.wallapop.com/1.jpg")
    ).toDF("id", "title", "modification_date", "year", "month", "day", "image_url")

    val pisosDf = Seq(
      ("p-1", "Pisos", Date.valueOf("2024-01-01"), 2024, 1, 1, "https://fotos.imghs.net/1.jpg")
    ).toDF("id", "title", "modification_date", "year", "month", "day", "image_url")

    val fotocasaDf = Seq(
      ("f-1", "Foto", Date.valueOf("2024-01-01"), 2024, 1, 1, "https://img.fotocasa.es/1.jpg")
    ).toDF("id", "title", "modification_date", "year", "month", "day", "image_url")

    val result = PropertiesFullTransformer.transform(wallapopDf, pisosDf, fotocasaDf)
    result.count() shouldEqual 3

    val rows = result.collect().map(r => (r.getAs[String]("id"), r.getAs[String]("image_url"))).toMap
    rows("w-1") shouldEqual "https://cdn.wallapop.com/1.jpg"
    rows("p-1") shouldEqual "https://fotos.imghs.net/1.jpg"
    rows("f-1") shouldEqual "https://img.fotocasa.es/1.jpg"
  }

}
