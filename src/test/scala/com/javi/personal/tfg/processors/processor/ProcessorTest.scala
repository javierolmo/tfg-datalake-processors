package com.javi.personal.tfg.processors.processor

import com.javi.personal.tfg.processors.processor.etls.{PropertiesFullProcessor, WallapopProperties}
import com.javi.personal.tfg.processors.utils.DataSourceProvider
import com.javi.personal.tfg.processors.utils.writers.SparkWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date
import java.time.LocalDate

class ProcessorTest extends AnyFlatSpec with Matchers {

  private implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("ProcessorTest")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  class TestWriter extends SparkWriter() {
    var written: Boolean = false
    override def write(dataFrame: DataFrame)(implicit spark: SparkSession): Unit = {
      written = true
    }
  }

  class StubDataSourceProvider(val wallapopDf: DataFrame, val provincesDf: DataFrame) extends DataSourceProvider {
    override def readSilver(source: String, datasetName: String)(implicit spark: SparkSession): DataFrame = {
      if (source == "opendatasoft") provincesDf else spark.emptyDataFrame
    }
    override def readSilver(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): DataFrame = {
      if (source == "wallapop") wallapopDf else spark.emptyDataFrame
    }
    override def readSilverOption(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): Option[DataFrame] = None
    override def readGold(dataset: ProcessedTables, dateOption: Option[LocalDate])(implicit spark: SparkSession): DataFrame = spark.emptyDataFrame
    override def readGoldOption(dataset: ProcessedTables, dateOption: Option[LocalDate])(implicit spark: SparkSession): Option[DataFrame] = None
  }

  it should "execute processor using customWriter and return aligned DataFrame" in {
    val wallapopDf = Seq(
      ("item-1", "Test item", 100000, 70, 2, 1, "Madrid", "ES", 28001, "Madrid", "sale", "flat", "Desc", "2024-01-01", "slug-1", "2024-01-01", 40.0, -3.0)
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

    val provincesDf = Seq((28, "Madrid")).toDF("codigo", "provincia")

    val stubProvider = new StubDataSourceProvider(wallapopDf, provincesDf)
    val testWriter = new TestWriter()
    val config = ProcessorConfig(ProcessedTables.WALLAPOP_PROPERTIES, LocalDate.of(2024, 1, 1), "dummy/path")

    val processor = new WallapopProperties(config, stubProvider, Some(testWriter))
    val resultDf = processor.execute()

    testWriter.written shouldEqual true
    resultDf.count() shouldEqual 1
    resultDf.columns should contain ("id")
    resultDf.columns should contain ("link")
    resultDf.columns should contain ("image_url")
    resultDf.first().getAs[String]("id") shouldEqual "item-1"
  }

  it should "execute fotocasa processor and return aligned DataFrame with properties_full schema" in {
    val fotocasaDf = Seq(
      (2, 0L, 42.235, -8.719, "2026-09-27 14:42:20", 4, 190446371L, 236, "Vigo", "comprar", 750000, "pontevedra-provincia", "47 DAYS", "Flat", "viviendas", "Centro", "https://url")
    ).toDF(
      "baños", "coordenadas__accuracy", "coordenadas__latitude", "coordenadas__longitude",
      "fecha_scraping", "habitaciones", "id", "metros", "municipio", "operacion",
      "precio", "provincia", "publicado_hace", "tipo_detalle", "tipo_inmueble", "ubicacion", "url"
    )

    val stubProvider = new DataSourceProvider {
      override def readSilver(source: String, datasetName: String)(implicit spark: SparkSession): DataFrame = spark.emptyDataFrame
      override def readSilver(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): DataFrame = fotocasaDf
      override def readSilverOption(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): Option[DataFrame] = None
      override def readGold(dataset: ProcessedTables, dateOption: Option[LocalDate])(implicit spark: SparkSession): DataFrame = spark.emptyDataFrame
      override def readGoldOption(dataset: ProcessedTables, dateOption: Option[LocalDate])(implicit spark: SparkSession): Option[DataFrame] = None
    }
    val testWriter = new TestWriter()
    val config = ProcessorConfig(ProcessedTables.FOTOCASA_PROPERTIES, LocalDate.of(2026, 9, 27), "dummy/path")

    val processor = Processor.build(config, stubProvider, Some(testWriter))
    val resultDf = processor.execute()

    testWriter.written shouldEqual true
    resultDf.count() shouldEqual 1
    resultDf.columns should contain theSameElementsAs Seq(
      "id", "title", "price", "surface", "rooms", "bathrooms", "link", "source",
      "creation_date", "elevator", "garage", "garden", "city", "country", "postal_code",
      "province", "region", "modification_date", "operation", "pool", "description",
      "terrace", "type", "latitude", "longitude", "image_url"
    )
    resultDf.first().getAs[String]("id") shouldEqual "190446371"
    resultDf.first().getAs[String]("source") shouldEqual "fotocasa"
    resultDf.first().getAs[String]("operation") shouldEqual "SELL"
    resultDf.first().getAs[String]("type") shouldEqual "FLAT"
  }

  it should "execute properties_full processor ensuring correct DateType for creation_date, modification_date, and load_date" in {
    val inputSchema = StructType(Seq(
      StructField("id", StringType),
      StructField("title", StringType),
      StructField("price", IntegerType),
      StructField("surface", IntegerType),
      StructField("rooms", IntegerType),
      StructField("bathrooms", IntegerType),
      StructField("link", StringType),
      StructField("source", StringType),
      StructField("creation_date", DateType),
      StructField("elevator", BooleanType),
      StructField("garage", BooleanType),
      StructField("garden", BooleanType),
      StructField("city", StringType),
      StructField("country", StringType),
      StructField("postal_code", IntegerType),
      StructField("province", StringType),
      StructField("region", StringType),
      StructField("modification_date", DateType),
      StructField("operation", StringType),
      StructField("pool", BooleanType),
      StructField("description", StringType),
      StructField("terrace", BooleanType),
      StructField("type", StringType),
      StructField("latitude", DoubleType),
      StructField("longitude", DoubleType),
      StructField("image_url", StringType),
      StructField("year", IntegerType),
      StructField("month", IntegerType),
      StructField("day", IntegerType)
    ))

    val rowData = Row(
      "w-1", "Piso en Sol", 250000, 80, 2, 1, "https://link-1", "wallapop", Date.valueOf("2026-09-01"),
      false, false, false, "Madrid", "ES", 28013, "Madrid", "Comunidad de Madrid",
      Date.valueOf("2026-09-25"), "SELL", false, "Desc", false, "FLAT", 40.41, -3.70, "https://img.wallapop.com/item.jpg", 2026, 9, 30
    )

    val wallapopGold = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(rowData)),
      inputSchema
    )

    val stubProvider = new DataSourceProvider {
      override def readSilver(source: String, datasetName: String)(implicit spark: SparkSession): DataFrame = spark.emptyDataFrame
      override def readSilver(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): DataFrame = spark.emptyDataFrame
      override def readSilverOption(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): Option[DataFrame] = None
      override def readGold(dataset: ProcessedTables, dateOption: Option[LocalDate])(implicit spark: SparkSession): DataFrame = spark.emptyDataFrame
      override def readGoldOption(dataset: ProcessedTables, dateOption: Option[LocalDate])(implicit spark: SparkSession): Option[DataFrame] = {
        if (dataset == ProcessedTables.WALLAPOP_PROPERTIES) Some(wallapopGold)
        else None
      }
    }

    val testWriter = new TestWriter()
    val config = ProcessorConfig(ProcessedTables.PROPERTIES_FULL, LocalDate.of(2026, 9, 30), "dummy/path")

    val processor = Processor.build(config, stubProvider, Some(testWriter))
    val resultDf = processor.execute()

    testWriter.written shouldEqual true
    resultDf.count() shouldEqual 1

    val schema = resultDf.schema
    schema("creation_date").dataType shouldEqual DateType
    schema("modification_date").dataType shouldEqual DateType
    schema("load_date").dataType shouldEqual DateType
    schema("image_url").dataType shouldEqual StringType

    val row = resultDf.first()
    row.getAs[Date]("creation_date") shouldEqual Date.valueOf("2026-09-01")
    row.getAs[Date]("modification_date") shouldEqual Date.valueOf("2026-09-25")
    row.getAs[Date]("load_date") shouldEqual Date.valueOf("2026-09-30")
    row.getAs[Int]("price") shouldEqual 250000
    row.getAs[String]("source") shouldEqual "wallapop"
    row.getAs[String]("image_url") shouldEqual "https://img.wallapop.com/item.jpg"
  }

}
