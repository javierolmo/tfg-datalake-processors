# Changelog

## [1.5.1] - 2026-10-04
### Added
- Propagación de `image_url` a `properties_full` y soporte para nuevos campos (`elevator`, `garage`, `floor`) en cleaner y processors.

## [1.5.0] - 2026-10-02
### Changed
- Renombrado del proyecto a `tfg-datalake-processors` y actualización de paquetes base a `com.javi.personal.tfg.processors`.

## [1.4.3] - 2026-10-02
### Fixed
- Corrección y recuperación de fechas fuera de rango en Wallapop y sanitizado en properties_full para compatibilidad con Synapse Serverless.

## [1.4.2] - 2026-10-02
### Changed
- Estandarización de tipos, operaciones y geografía, y mejoras en parseo de fechas y consistencia de esquemas.

## [1.4.1] - 2026-09-29
### Fixed
- Usar `try_cast` nativo en `FieldCleaner` para evitar fallo al castear valores no numéricos con ANSI SQL activo

## [1.4.0] - 2026-09-28
### Added
- Integración de nueva fuente Fotocasa en cleaner y processor, cálculo espacial de código postal y consolidación en properties_full con load_date

## [1.3.2] - 2026-09-26
### Changed
- Refactorización de `processor`: desacoplamiento de transformers, configuración tipada y mejoras para testeo

## [1.3.1] - 2026-09-26
### Changed
- Soporte para Databricks Runtime 19 (Spark 4.2.0 y Scala 2.13.18) y optimización de dependencias

## [1.3.0] - 2026-02-22
### Added
- Add configuration for properties_full processor, and new argument -c (--coalesce) for procesor

## [1.2.8] - 2026-02-21
### Changed
- Disable DB creations to prevent using databricks unity catalog

## [1.2.7] - 2026-02-21
### Changed
- Arreglado PisosProcessor
- Cruce de datos entre pisos y polígonos de codigos postales para obtener localización

## [1.2.6] - 2026-02-15
### Changed
- Actualización esquema pisos.com
- Cambio de comportamiento cleaner: Si un campo no existe en origen, se añade al destino con valor nulo en lugar de terminar con error.

## [1.2.5] - 2026-02-15
### Changed
- Adecuación procesos a nueva estructura del datalake

## [1.2.4] - 2026-02-15
### Changed
- Arreglado el workflow de despliegue de relases.

---

> Este changelog documenta los análisis y sugerencias realizados sobre los módulos principales del proyecto, así como el incremento de versión.
