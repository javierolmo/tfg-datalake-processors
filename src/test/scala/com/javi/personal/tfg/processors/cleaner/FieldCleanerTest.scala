package com.javi.personal.tfg.processors.cleaner

import com.javi.personal.tfg.processors.cleaner.model.Transformations
import org.apache.spark.sql.functions.{array, array_except, col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.language.postfixOps

class FieldCleanerTest extends AnyFlatSpec {

  private val spark = SparkSession.builder()
    .master("local[*]")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  it should "clean string correctly" in {
    val input: String = "some_value"
    val cleaner = FieldCleaner("some_field", StringType)

    val (dataType, value) = executeCleaner(input, cleaner)

    value shouldEqual Some("some_value")
    dataType shouldEqual StringType
  }

  it should "clean integer correctly" in {
    val input: String = "123123"
    val cleaner = FieldCleaner("some_field", IntegerType)

    val (dataType, value) = executeCleaner(input, cleaner)

    value shouldEqual Some(123123)
    dataType shouldEqual IntegerType
  }

  it should "clean integer error when input is not an integer" in {
    val input: String = "sd123gasd"
    val cleaner = FieldCleaner("some_field", IntegerType)

    val (dataType, value) = executeCleaner(input, cleaner)

    value shouldEqual None
    dataType shouldEqual IntegerType
  }

  it should "Default value should be taken when input field is null" in {
    val input: String = null
    val cleaner = FieldCleaner("some_field", IntegerType, defaultValue = Some(0))

    val (dataType, value) = executeCleaner(input, cleaner)

    value shouldEqual Some(0)
    dataType shouldEqual IntegerType
  }

  it should "Default value should not be taken when cast fails" in {
    val input: String = "sd123gasd"
    val cleaner = FieldCleaner("some_field", IntegerType, defaultValue = Some(0))

    val (dataType, value) = executeCleaner(input, cleaner)

    value shouldEqual None
    dataType shouldEqual IntegerType
  }

  it should "Transformation should be applied before cast" in {
    val input: String = "sd123gasd"
    val cleaner = FieldCleaner("some_field", IntegerType, transform = Some(Transformations.removeNonNumeric))

    val (dataType, value) = executeCleaner(input, cleaner)

    value shouldEqual Some(123)
    dataType shouldEqual IntegerType
  }

  it should "handle empty string or non-numeric input without erroring when casting to integer" in {
    val input: String = "no_numbers"
    val cleaner = FieldCleaner("some_field", IntegerType, transform = Some(Transformations.removeNonNumeric))

    val (dataType, value) = executeCleaner(input, cleaner)

    value shouldEqual None
    dataType shouldEqual IntegerType
  }

  it should "Filter should be applied before cast" in {
    val input: String = "some_value"
    val cleaner = FieldCleaner("some_field", StringType, filter = Some(_.isin("some_value")))

    val (dataType, value) = executeCleaner(input, cleaner)

    value shouldEqual Some("some_value")
    dataType shouldEqual StringType
  }

  it should "Error should be returned when filter does not match (string comparation)" in {
    val input: String = "some_value"
    val cleaner = FieldCleaner("some_field", StringType, filter = Some(_.isin("some_other_value")))

    val (dataType, value) = executeCleaner(input, cleaner)

    value shouldEqual None
    dataType shouldEqual StringType
  }

  it should "Error should be returned filter does not match (null case)" in {
    val input: String = null
    val cleaner = FieldCleaner("some_field", StringType, filter = Some(_.isNotNull))

    val (dataType, value) = executeCleaner(input, cleaner)

    value shouldEqual None
    dataType shouldEqual StringType
  }

  "Regression: Fotocasa non-numeric fields" should "cast 'No disponible' with removeNonNumeric as null without Error casting" in {
    val input: String = "No disponible"
    val cleaner = FieldCleaner("baños", IntegerType, transform = Some(Transformations.removeNonNumeric))

    val (dataType, value, errors) = executeCleanerWithErrors(input, cleaner)

    dataType shouldEqual IntegerType
    value shouldEqual None
    errors shouldBe empty
  }

  "Regression: Wallapop international postal codes" should "report Error casting for non-numeric postal codes like '4900-809'" in {
    val input: String = "4900-809"
    val cleaner = FieldCleaner("location__postal_code", IntegerType)

    val (dataType, value, errors) = executeCleanerWithErrors(input, cleaner)

    dataType shouldEqual IntegerType
    value shouldEqual None
    errors should not be empty
    errors.head.getAs[String]("message") shouldEqual "Error casting"
    errors.head.getAs[String]("fieldName") shouldEqual "location__postal_code"
  }

  "Regression: Pisos date handling" should "clean date in array format '[2026,2,19]'" in {
    val input: String = "[2026,2,19]"
    val cleaner = FieldCleaner("lastUpdateDate", DateType, transform = Some(Transformations.parseDate))

    val (dataType, value, errors) = executeCleanerWithErrors(input, cleaner)

    dataType shouldEqual DateType
    value shouldEqual Some(java.sql.Date.valueOf("2026-02-19"))
    errors shouldBe empty
  }

  it should "clean date in spaced array format '[2026, 2, 19]' successfully" in {
    val input: String = "[2026, 2, 19]"
    val cleaner = FieldCleaner("lastUpdateDate", DateType, transform = Some(Transformations.parseDate))

    val (dataType, value, errors) = executeCleanerWithErrors(input, cleaner)

    dataType shouldEqual DateType
    value shouldEqual Some(java.sql.Date.valueOf("2026-02-19"))
    errors shouldBe empty
  }

  it should "clean date in Spanish format '19/02/2026' successfully" in {
    val input: String = "19/02/2026"
    val cleaner = FieldCleaner("lastUpdateDate", DateType, transform = Some(Transformations.parseDate))

    val (dataType, value, errors) = executeCleanerWithErrors(input, cleaner)

    dataType shouldEqual DateType
    value shouldEqual Some(java.sql.Date.valueOf("2026-02-19"))
    errors shouldBe empty
  }

  it should "clean standard ISO date '2026-02-19' successfully without errors" in {
    val input: String = "2026-02-19"
    val cleaner = FieldCleaner("lastUpdateDate", DateType, transform = Some(Transformations.parseDate))

    val (dataType, value, errors) = executeCleanerWithErrors(input, cleaner)

    dataType shouldEqual DateType
    value shouldEqual Some(java.sql.Date.valueOf("2026-02-19"))
    errors shouldBe empty
  }

  it should "report Error casting when date format is truly invalid" in {
    val input: String = "invalid-date-string"
    val cleaner = FieldCleaner("lastUpdateDate", DateType, transform = Some(Transformations.parseDate))

    val (dataType, value, errors) = executeCleanerWithErrors(input, cleaner)

    dataType shouldEqual DateType
    value shouldEqual None
    errors should not be empty
    errors.head.getAs[String]("message") shouldEqual "Error casting"
  }

  "Timestamp handling in FieldCleaner" should "clean ISO-8601 timestamp with Z" in {
    val input: String = "2026-08-09T10:00:00.000Z"
    val cleaner = FieldCleaner("created_at", TimestampType, transform = Some(Transformations.parseTimestamp))

    val (dataType, value, errors) = executeCleanerWithErrors(input, cleaner)

    dataType shouldEqual TimestampType
    value should not be empty
    errors shouldBe empty
  }

  it should "clean epoch milliseconds as timestamp" in {
    val input: String = "1727733734000"
    val cleaner = FieldCleaner("created_at", TimestampType, transform = Some(Transformations.parseTimestamp))

    val (dataType, value, errors) = executeCleanerWithErrors(input, cleaner)

    dataType shouldEqual TimestampType
    value should not be empty
    errors shouldBe empty
  }

  it should "clean Spanish timestamp format '27/09/2026 14:42:20'" in {
    val input: String = "27/09/2026 14:42:20"
    val cleaner = FieldCleaner("fecha_scraping", TimestampType, transform = Some(Transformations.parseTimestamp))

    val (dataType, value, errors) = executeCleanerWithErrors(input, cleaner)

    dataType shouldEqual TimestampType
    value should not be empty
    errors shouldBe empty
  }

  "Boolean handling in FieldCleaner" should "clean true string as boolean true" in {
    val cleaner = FieldCleaner("elevator", BooleanType, transform = Some(Transformations.parseBoolean))
    val (dataType, value, errors) = executeCleanerWithErrors("true", cleaner)

    dataType shouldEqual BooleanType
    value shouldEqual Some(true)
    errors shouldBe empty
  }

  it should "clean false string as boolean false" in {
    val cleaner = FieldCleaner("elevator", BooleanType, transform = Some(Transformations.parseBoolean))
    val (dataType, value, errors) = executeCleanerWithErrors("false", cleaner)

    dataType shouldEqual BooleanType
    value shouldEqual Some(false)
    errors shouldBe empty
  }

  it should "clean null as boolean false without errors" in {
    val cleaner = FieldCleaner("elevator", BooleanType, transform = Some(Transformations.parseBoolean))
    val (dataType, value, errors) = executeCleanerWithErrors(null, cleaner)

    dataType shouldEqual BooleanType
    value shouldEqual Some(false)
    errors shouldBe empty
  }

  it should "clean uncastable string as boolean false without errors" in {
    val cleaner = FieldCleaner("parking", BooleanType, transform = Some(Transformations.parseBoolean))
    val (dataType, value, errors) = executeCleanerWithErrors("uncastable_random_value", cleaner)

    dataType shouldEqual BooleanType
    value shouldEqual Some(false)
    errors shouldBe empty
  }

  "Floor handling in FieldCleaner" should "clean numeric floor string as integer" in {
    val cleaner = FieldCleaner("floor", IntegerType, transform = Some(Transformations.removeNonNumeric))
    val (dataType, value, errors) = executeCleanerWithErrors("3", cleaner)

    dataType shouldEqual IntegerType
    value shouldEqual Some(3)
    errors shouldBe empty
  }

  it should "clean floor string with ordinals as integer" in {
    val cleaner = FieldCleaner("floor", IntegerType, transform = Some(Transformations.removeNonNumeric))
    val (dataType, value, errors) = executeCleanerWithErrors("3º", cleaner)

    dataType shouldEqual IntegerType
    value shouldEqual Some(3)
    errors shouldBe empty
  }

  it should "clean null floor as None without errors" in {
    val cleaner = FieldCleaner("floor", IntegerType, transform = Some(Transformations.removeNonNumeric))
    val (dataType, value, errors) = executeCleanerWithErrors(null, cleaner)

    dataType shouldEqual IntegerType
    value shouldEqual None
    errors shouldBe empty
  }

  private def executeCleaner(input: String, cleaner: FieldCleaner): (DataType, Option[Any]) = {
    val (dt, value, _) = executeCleanerWithErrors(input, cleaner)
    (dt, value)
  }

  private def executeCleanerWithErrors(input: String, cleaner: FieldCleaner): (DataType, Option[Any], Seq[Row]) = {
    val df: DataFrame = Seq(input).toDF("some_field")
    val (errors, result) = cleaner.clean(col("some_field"))
    val cleanedDF = df
      .withColumn("errors", array_except(errors, array(lit(null))))
      .withColumn("result", result)
    val head = cleanedDF.collect().head
    val errorsSeq: Seq[Row] = Option(head.getAs[scala.collection.Seq[Row]]("errors"))
      .map(_.toSeq)
      .getOrElse(Seq.empty)
    (
      cleanedDF.schema("result").dataType,
      Option(head.getAs[Any]("result")),
      errorsSeq
    )
  }

}
