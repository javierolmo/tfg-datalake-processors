package com.javi.personal.tfg.processors.cleaner.model

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class TransformationsTest extends AnyFlatSpec with Matchers {

  private val spark = SparkSession.builder().master("local[*]").getOrCreate()

  import spark.implicits._

  "removeNonNumeric" should "keep only numbers" in {
    val input = "as125asf12"
    val transformation:Column => Column = Transformations.removeNonNumeric

    val value = executeTransformation(input, transformation)

    value should be (Some("12512"))
  }

  it should "remove line breaks" in {
    val input = "some\n value\n with\n line\n breaks"
    val transformation:Column => Column = Transformations.removeLineBreaks

    val value = executeTransformation(input, transformation)

    value should be (Some("some value with line breaks"))
  }

  "parseTimestamp" should "correctly parse 13-digit epoch milliseconds without overflow" in {
    val input = "1685952314128"
    val transformation: Column => Column = Transformations.parseTimestamp

    val value = executeTransformation(input, transformation)

    value shouldBe defined
    val ts = value.get.asInstanceOf[Timestamp]
    val year = ts.toLocalDateTime.getYear
    year shouldEqual 2023
  }

  it should "correctly parse 10-digit epoch seconds" in {
    val input = "1685952314"
    val transformation: Column => Column = Transformations.parseTimestamp

    val value = executeTransformation(input, transformation)

    value shouldBe defined
    val ts = value.get.asInstanceOf[Timestamp]
    val year = ts.toLocalDateTime.getYear
    year shouldEqual 2023
  }

  "parseBoolean" should "parse true, 1, and affirmative strings as true" in {
    for (affirmative <- Seq("true", "TRUE", "True", "1", "si", "sí", "yes", "t")) {
      val value = executeTransformation(affirmative, Transformations.parseBoolean)
      value should be (Some(true))
    }
  }

  it should "parse false, 0, and negative strings as false" in {
    for (negative <- Seq("false", "FALSE", "False", "0", "no", "f")) {
      val value = executeTransformation(negative, Transformations.parseBoolean)
      value should be (Some(false))
    }
  }

  it should "parse null as false" in {
    val value = executeTransformation(null, Transformations.parseBoolean)
    value should be (Some(false))
  }

  it should "parse uncastable strings as false by default" in {
    val value = executeTransformation("random_uncastable_text", Transformations.parseBoolean)
    value should be (Some(false))
  }

  "extractImageUrl" should "extract URL from direct url string" in {
    val input = "https://cdn.wallapop.com/images/10420/item.jpg"
    val value = executeTransformation(input, Transformations.extractImageUrl)
    value shouldEqual Some("https://cdn.wallapop.com/images/10420/item.jpg")
  }

  it should "extract URL from nested struct or array representation" in {
    val input = "[Row(urls=Row(big=https://cdn.wallapop.com/images/10420/i123.jpg?pictureSize=W800, small=https://...))]"
    val value = executeTransformation(input, Transformations.extractImageUrl)
    value shouldEqual Some("https://cdn.wallapop.com/images/10420/i123.jpg?pictureSize=W800")
  }

  it should "return empty string when no URL is found" in {
    val input = "[]"
    val value = executeTransformation(input, Transformations.extractImageUrl)
    value shouldEqual Some("")
  }

  it should "handle null input safely" in {
    val value = executeTransformation(null, Transformations.extractImageUrl)
    value shouldEqual None
  }

  private def executeTransformation(input: String, transformation: Column => Column): Option[Any] = {
    val df: DataFrame = Seq(input).toDF("some_field")
    val cleanedDF = df.select(transformation(col("some_field")))
    val result: Row = cleanedDF.collect()(0)
    Option(result(0))
  }

}
