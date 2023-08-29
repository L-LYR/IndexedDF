package indexeddataframe

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.log4j.Logger
import org.apache.spark.sql.catalog.Catalog

case class Table(
    name: String,
    partitionColumns: Seq[String],
    fields: StructField*
) {
  val schema = StructType(fields)

  private val log = Logger.getLogger(getClass.getName)

  def unPartitioned: Table = {
    Table(name, Nil, fields: _*)
  }

  def convertTypes(
      useDoubleForDecimal: Boolean,
      useStringForDate: Boolean
  ): Table = {
    val newFields = fields.map { field =>
      val newDataType = field.dataType match {
        case decimal: DecimalType if useDoubleForDecimal => DoubleType
        case date: DateType if useStringForDate          => StringType
        case other                                       => other
      }
      field.copy(dataType = newDataType)
    }

    Table(name, partitionColumns, newFields: _*)
  }

  def loadData(
      sqlContext: SQLContext,
      dataLocation: String,
      dataFormat: String
  ): DataFrame = {
    sqlContext.read.format(dataFormat).load(s"$dataLocation/${name}")
  }

  def createTemporaryTable(
      sqlContext: SQLContext,
      dataLocation: String,
      dataFormat: String
  ): Unit = {
    loadData(sqlContext, dataLocation, dataFormat).createOrReplaceTempView(name)
  }

  def createExternalTable(
      catalog: Catalog,
      dataLocation: String,
      databaseName: String,
      dataFormat: String,
      discoverPartition: Boolean = true
  ): Option[DataFrame] = {
    val tableName = s"$databaseName.$name"
    val tableDataLocation = s"${dataLocation}/${name}"
    if (catalog.tableExists(tableName)) {
      log.warn(s"$tableName already exists")
      None
    } else {
      val df = catalog.createTable(tableName, tableDataLocation, dataFormat)
      if (partitionColumns.nonEmpty && discoverPartition) {
        log.info(s"$tableName recover partitions")
        catalog.recoverPartitions(tableName)
      }
      Some(df)
    }
  }

}
