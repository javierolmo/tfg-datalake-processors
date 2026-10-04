package com.javi.personal.tfg.processors.cleaner.model

import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MetadataCatalogTest extends AnyFlatSpec with Matchers {

  "MetadataCatalog.default" should "contain zip_codes in default catalog with correct types" in {
    val catalog = MetadataCatalog.default()
    val zipCodes = catalog.findByCatalogItem("zip_codes")

    zipCodes should be ('defined)
    val fields = zipCodes.get.fields
    val fieldNames = fields.map(_.name)
    fieldNames should contain theSameElementsAs Seq(
      "codigo_postal", "municipio_id", "coordinates", "nombre", "provincia"
    )

    val coordinatesField = fields.find(_.name == "coordinates").get
    coordinatesField.dataType shouldEqual ArrayType(StructType(Seq(
      StructField("latitude", DoubleType),
      StructField("longitude", DoubleType)
    )))
  }

  it should "contain all expected datasets in default catalog" in {
    val catalog = MetadataCatalog.default()
    catalog.availableIds() should contain theSameElementsAs Seq(
      "wallapop_properties_old",
      "wallapop_properties",
      "fotocasa_properties",
      "opendatasoft_provincias-espanolas",
      "pisos_properties",
      "zipCodes".replaceAll("zipCodes", "zip_codes"),
      "wallapop_properties_2"
    )
  }

  it should "contain fotocasa_properties in default catalog with new fields" in {
    val catalog = MetadataCatalog.default()
    val fotocasa = catalog.findByCatalogItem("fotocasa_properties")

    fotocasa should be ('defined)
    val fields = fotocasa.get.fields
    val fieldNames = fields.map(_.name)
    fieldNames should contain theSameElementsAs Seq(
      "baños", "coordenadas__accuracy", "coordenadas__latitude", "coordenadas__longitude",
      "fecha_scraping", "habitaciones", "id", "metros", "municipio", "operacion",
      "precio", "provincia", "publicado_hace", "tipo_detalle", "tipo_inmueble",
      "ubicacion", "url", "imagen_portada", "ascensor", "parking"
    )

    val ascensorField = fields.find(_.name == "ascensor").get
    ascensorField.dataType shouldEqual BooleanType
    ascensorField.transform shouldBe 'defined

    val parkingField = fields.find(_.name == "parking").get
    parkingField.dataType shouldEqual BooleanType
    parkingField.transform shouldBe 'defined

    val imagenField = fields.find(_.name == "imagen_portada").get
    imagenField.dataType shouldEqual StringType
  }

  it should "contain wallapop_properties_2 in default catalog with elevator, garage, parking, and images" in {
    val catalog = MetadataCatalog.default()
    val wallapop = catalog.findByCatalogItem("wallapop_properties_2")

    wallapop should be ('defined)
    val fields = wallapop.get.fields
    val fieldNames = fields.map(_.name)
    fieldNames should contain allOf ("elevator", "garage", "parking", "images")

    val elevatorField = fields.find(_.name == "elevator").get
    elevatorField.dataType shouldEqual BooleanType
    elevatorField.transform shouldBe 'defined

    val garageField = fields.find(_.name == "garage").get
    garageField.dataType shouldEqual BooleanType
    garageField.transform shouldBe 'defined

    val parkingField = fields.find(_.name == "parking").get
    parkingField.dataType shouldEqual BooleanType
    parkingField.transform shouldBe 'defined

    val imagesField = fields.find(_.name == "images").get
    imagesField.dataType shouldEqual StringType
    imagesField.transform shouldBe 'defined
  }

  it should "contain pisos_properties in default catalog with elevator, parking, floor and all fields" in {
    val catalog = MetadataCatalog.default()
    val pisos = catalog.findByCatalogItem("pisos_properties")

    pisos should be ('defined)
    val fields = pisos.get.fields
    val fieldNames = fields.map(_.name)
    fieldNames should contain theSameElementsAs Seq(
      "id", "title", "price", "url", "fullDescription", "rooms", "bathrooms",
      "surface", "floor", "imageUrl", "lastUpdateDate", "latitude", "longitude",
      "propertyType", "location", "elevator", "parking"
    )

    val elevatorField = fields.find(_.name == "elevator").get
    elevatorField.dataType shouldEqual BooleanType
    elevatorField.transform shouldBe 'defined

    val parkingField = fields.find(_.name == "parking").get
    parkingField.dataType shouldEqual BooleanType
    parkingField.transform shouldBe 'defined

    val floorField = fields.find(_.name == "floor").get
    floorField.dataType shouldEqual IntegerType
  }

}
