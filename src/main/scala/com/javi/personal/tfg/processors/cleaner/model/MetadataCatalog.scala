package com.javi.personal.tfg.processors.cleaner.model

import com.javi.personal.tfg.processors.cleaner.FieldCleaner
import org.apache.spark.sql.types._

case class MetadataCatalog(items: Seq[CleanerMetadata]) {
    def findByCatalogItem(id: String): Option[CleanerMetadata] = items.find(_.id == id)
    def availableIds(): Seq[String] = items.map(_.id)
}

object MetadataCatalog {

  def default(): MetadataCatalog = MetadataCatalog(
    items = Seq(
      wallapopPropertiesOld,
      wallapopProperties,
      fotocasaProperties,
      provinciasEspanolas,
      pisosProperties,
      zipCodes,
      wallapopProperties2
    )
  )

  private val zipCodes: CleanerMetadata = CleanerMetadata(
    id = "zip_codes",
    fields = Seq(
      FieldCleaner("codigo_postal", IntegerType),
      FieldCleaner("municipio_id", IntegerType, filter = Some(_.isNotNull)),
      FieldCleaner("coordinates", ArrayType(StructType(Seq(
        StructField("latitude", DoubleType),
        StructField("longitude", DoubleType)
      ))), filter = Some(_.isNotNull)),
      FieldCleaner("nombre", StringType, filter = Some(_.isNotNull)),
      FieldCleaner("provincia", StringType, filter = Some(_.isNotNull)),
    )
  )

  private val wallapopPropertiesOld: CleanerMetadata = CleanerMetadata(
    id = "wallapop_properties_old",
    fields = Seq(
      FieldCleaner("bathrooms", IntegerType),
      FieldCleaner("category_id", IntegerType, filter = Some(_.equalTo("200"))),
      FieldCleaner("condition", StringType),
      FieldCleaner("creation_date", TimestampType, transform = Some(Transformations.parseTimestamp)),
      FieldCleaner("currency", StringType),
      FieldCleaner("distance", DoubleType),
      FieldCleaner("elevator", BooleanType),
      FieldCleaner("favorited", BooleanType),
      FieldCleaner("flags__banned", BooleanType),
      FieldCleaner("flags__expired", BooleanType),
      FieldCleaner("flags__onhold", BooleanType),
      FieldCleaner("flags__pending", BooleanType),
      FieldCleaner("flags__reserved", BooleanType),
      FieldCleaner("flags__sold", BooleanType),
      FieldCleaner("garage", BooleanType),
      FieldCleaner("garden", BooleanType),
      FieldCleaner("id", StringType),
      FieldCleaner("images", StringType),
      FieldCleaner("location__city", StringType),
      FieldCleaner("location__country_code", StringType),
      FieldCleaner("location__postal_code", IntegerType),
      FieldCleaner("modification_date", TimestampType, transform = Some(Transformations.parseTimestamp)),
      FieldCleaner("operation", StringType, filter = Some(_.isNotNull)),
      FieldCleaner("pool", BooleanType),
      FieldCleaner("price", DoubleType),
      FieldCleaner("rooms", IntegerType),
      FieldCleaner("storytelling", StringType),
      FieldCleaner("surface", IntegerType),
      FieldCleaner("terrace", BooleanType),
      FieldCleaner("title", StringType),
      FieldCleaner("type", StringType, filter = Some(_.isNotNull)),
      FieldCleaner("user__id", StringType),
      FieldCleaner("visibility_flags__boosted", BooleanType),
      FieldCleaner("visibility_flags__bumped", BooleanType),
      FieldCleaner("visibility_flags__country_bumped", BooleanType),
      FieldCleaner("visibility_flags__highlighted", BooleanType),
      FieldCleaner("visibility_flags__urgent", BooleanType),
      FieldCleaner("web_slug", StringType),
      FieldCleaner("source", StringType),
      FieldCleaner("date", StringType),
    )
  )

  private val wallapopProperties2: CleanerMetadata = CleanerMetadata(
    id = "wallapop_properties_2",
    fields = Seq(
      FieldCleaner("category_id", IntegerType, filter = Some(_.equalTo("200"))),
      FieldCleaner("created_at", TimestampType, transform = Some(Transformations.parseTimestamp)),
      FieldCleaner("description", StringType),
      FieldCleaner("id", StringType),
      // FieldCleaner("images", ArrayType(null)),
      FieldCleaner("location__city", StringType),
      FieldCleaner("location__country_code", StringType),
      FieldCleaner("location__postal_code", IntegerType),
      FieldCleaner("location__latitude", DoubleType),
      FieldCleaner("location__longitude", DoubleType),
      FieldCleaner("location__region", StringType),
      FieldCleaner("location__region2", StringType),
      FieldCleaner("modified_at", TimestampType, transform = Some(Transformations.parseTimestamp)),
      FieldCleaner("price__amount", DoubleType),
      FieldCleaner("price__currency", StringType),
      // FieldCleaner("taxonomy", ArrayType(null)),
      FieldCleaner("title", StringType),
      FieldCleaner("type_attributes__bathrooms", IntegerType),
      FieldCleaner("type_attributes__operation", StringType),
      FieldCleaner("type_attributes__rooms", IntegerType),
      FieldCleaner("type_attributes__surface", IntegerType),
      FieldCleaner("type_attributes__type", StringType),
      FieldCleaner("user_id", StringType),
      FieldCleaner("web_slug", StringType)
    )
  )

  private val wallapopProperties: CleanerMetadata = CleanerMetadata(
    id = "wallapop_properties",
    fields = Seq(
      FieldCleaner("bathrooms", IntegerType),
      FieldCleaner("category_id", IntegerType, filter = Some(_.equalTo("200"))),
      FieldCleaner("condition", StringType),
      FieldCleaner("creation_date", TimestampType, transform = Some(Transformations.parseTimestamp)),
      FieldCleaner("currency", StringType),
      FieldCleaner("distance", DoubleType),
      FieldCleaner("elevator", BooleanType),
      FieldCleaner("favorited", BooleanType),
      FieldCleaner("flags__banned", BooleanType),
      FieldCleaner("flags__expired", BooleanType),
      FieldCleaner("flags__onhold", BooleanType),
      FieldCleaner("flags__pending", BooleanType),
      FieldCleaner("flags__reserved", BooleanType),
      FieldCleaner("flags__sold", BooleanType),
      FieldCleaner("garage", BooleanType),
      FieldCleaner("garden", BooleanType),
      FieldCleaner("id", StringType),
      FieldCleaner("images", StringType),
      FieldCleaner("location__city", StringType),
      FieldCleaner("location__country_code", StringType),
      FieldCleaner("location__postal_code", IntegerType),
      FieldCleaner("modification_date", TimestampType, transform = Some(Transformations.parseTimestamp)),
      FieldCleaner("operation", StringType, filter = Some(_.isNotNull)),
      FieldCleaner("pool", BooleanType),
      FieldCleaner("price", DoubleType),
      FieldCleaner("rooms", IntegerType),
      FieldCleaner("storytelling", StringType),
      FieldCleaner("surface", IntegerType),
      FieldCleaner("terrace", BooleanType),
      FieldCleaner("title", StringType),
      FieldCleaner("type", StringType, filter = Some(_.isNotNull)),
      FieldCleaner("user__id", StringType),
      FieldCleaner("visibility_flags__boosted", BooleanType),
      FieldCleaner("visibility_flags__bumped", BooleanType),
      FieldCleaner("visibility_flags__country_bumped", BooleanType),
      FieldCleaner("visibility_flags__highlighted", BooleanType),
      FieldCleaner("visibility_flags__urgent", BooleanType),
      FieldCleaner("web_slug", StringType),
      FieldCleaner("source", StringType),
      FieldCleaner("date", StringType),
    )
  )

  private val fotocasaProperties: CleanerMetadata = CleanerMetadata(
    id = "fotocasa_properties",
    fields = Seq(
      FieldCleaner("id", LongType),
      FieldCleaner("baños", IntegerType, transform = Some(Transformations.removeNonNumeric)),
      FieldCleaner("coordenadas__accuracy", LongType),
      FieldCleaner("coordenadas__latitude", DoubleType),
      FieldCleaner("coordenadas__longitude", DoubleType),
      FieldCleaner("fecha_scraping", TimestampType, transform = Some(Transformations.parseTimestamp)),
      FieldCleaner("habitaciones", IntegerType, transform = Some(Transformations.removeNonNumeric)),
      FieldCleaner("metros", IntegerType, transform = Some(Transformations.removeNonNumeric)),
      FieldCleaner("municipio", StringType),
      FieldCleaner("operacion", StringType),
      FieldCleaner("precio", IntegerType, transform = Some(Transformations.removeNonNumeric)),
      FieldCleaner("provincia", StringType),
      FieldCleaner("publicado_hace", StringType),
      FieldCleaner("tipo_detalle", StringType),
      FieldCleaner("tipo_inmueble", StringType),
      FieldCleaner("ubicacion", StringType),
      FieldCleaner("url", StringType)
    )
  )

  private val pisosProperties: CleanerMetadata = CleanerMetadata(
    id = "pisos_properties",
    fields = Seq(
      FieldCleaner("id", StringType),
      FieldCleaner("title", StringType, transform = Some(Transformations.removeLineBreaks)),
      FieldCleaner("price", IntegerType, transform = Some(Transformations.removeNonNumeric)),
      FieldCleaner("url", StringType),
      FieldCleaner("fullDescription", StringType, transform = Some(Transformations.removeLineBreaks)),
      FieldCleaner("rooms", IntegerType, transform = Some(Transformations.removeNonNumeric)),
      FieldCleaner("bathrooms", IntegerType, transform = Some(Transformations.removeNonNumeric)),
      FieldCleaner("surface", IntegerType, transform = Some(Transformations.removeNonNumeric)),
      FieldCleaner("floor", IntegerType, transform = Some(Transformations.removeNonNumeric)),
      FieldCleaner("imageUrl", StringType),
      FieldCleaner("lastUpdateDate", DateType, transform = Some(Transformations.parseDate)),
      FieldCleaner("latitude", DoubleType),
      FieldCleaner("longitude", DoubleType),
      FieldCleaner("propertyType", StringType),
      FieldCleaner("location", StringType),
      FieldCleaner("elevator", BooleanType, transform = Some(Transformations.parseBoolean)),
      FieldCleaner("parking", BooleanType, transform = Some(Transformations.parseBoolean))
    )
  )

  private val provinciasEspanolas: CleanerMetadata = CleanerMetadata(
    id = "opendatasoft_provincias-espanolas",
    fields = Seq(
      FieldCleaner("ccaa", StringType),
      FieldCleaner("cod_ccaa", IntegerType),
      FieldCleaner("codigo", IntegerType),
      FieldCleaner("geo_point_2d", StructType(Seq(
        StructField("lat", DoubleType),
        StructField("lon", DoubleType)
      ))),
      FieldCleaner("provincia", StringType)
    )
  )

}
