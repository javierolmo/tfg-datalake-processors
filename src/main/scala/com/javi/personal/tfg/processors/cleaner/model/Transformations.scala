package com.javi.personal.tfg.processors.cleaner.model

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Transformations {

  def removeNonNumeric(inputColumn: Column): Column = regexp_replace(inputColumn.cast(StringType), "[^0-9]", "")

  def removeLineBreaks(inputColumn: Column): Column = regexp_replace(inputColumn, "\\n", "")

  def millisecondsToTimestamp(inputColumn: Column): Column = from_unixtime(inputColumn / 1000)

  def parseBoolean(inputColumn: Column): Column = parseBooleanWithDefault(false)(inputColumn)

  def parseBooleanWithDefault(default: Boolean)(inputColumn: Column): Column = {
    val str = trim(lower(inputColumn.cast(StringType)))
    val parsedBool = when(str.isin("true", "t", "1", "si", "sí", "yes", "y"), lit(true))
      .when(str.isin("false", "f", "0", "no", "n"), lit(false))
      .otherwise(inputColumn.try_cast(BooleanType))
    coalesce(parsedBool, lit(default))
  }

  def extractImageUrl(inputColumn: Column): Column = {
    regexp_extract(inputColumn.cast(StringType), """(https?://[^\s"',\)\(\]\}]+)""", 1)
  }

  def parseDate(inputColumn: Column): Column = {
    val strCol = trim(inputColumn.cast(StringType))

    val arrayDate = when(strCol.rlike("""^\[\s*\d{4}\s*,\s*\d{1,2}\s*,\s*\d{1,2}\s*\]$"""),
      make_date(
        regexp_extract(strCol, """^\[\s*(\d{4})\s*,\s*(\d{1,2})\s*,\s*(\d{1,2})\s*\]$""", 1).cast(IntegerType),
        regexp_extract(strCol, """^\[\s*(\d{4})\s*,\s*(\d{1,2})\s*,\s*(\d{1,2})\s*\]$""", 2).cast(IntegerType),
        regexp_extract(strCol, """^\[\s*(\d{4})\s*,\s*(\d{1,2})\s*,\s*(\d{1,2})\s*\]$""", 3).cast(IntegerType)
      )
    )

    coalesce(
      arrayDate,
      to_date(inputColumn),
      to_date(strCol, "dd/MM/yyyy"),
      to_date(strCol, "dd-MM-yyyy"),
      to_date(strCol, "yyyy/MM/dd")
    )
  }

  def parseTimestamp(inputColumn: Column): Column = {
    val strCol = trim(inputColumn.cast(StringType))

    val epochMillis = when(strCol.rlike("""^\d{13}$"""),
      from_unixtime(strCol.cast(LongType) / 1000).cast(TimestampType)
    )

    val epochSecs = when(strCol.rlike("""^\d{10}$"""),
      from_unixtime(strCol.cast(LongType)).cast(TimestampType)
    )

    coalesce(
      epochMillis,
      epochSecs,
      to_timestamp(inputColumn),
      to_timestamp(strCol, "dd/MM/yyyy HH:mm:ss"),
      to_timestamp(strCol, "dd/MM/yyyy"),
      to_timestamp(strCol, "dd-MM-yyyy HH:mm:ss"),
      to_timestamp(strCol, "yyyy/MM/dd HH:mm:ss"),
      parseDate(inputColumn).cast(TimestampType)
    )
  }

}
