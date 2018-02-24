/**
 * Copyright 2016 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zuinnote.spark.office.excel

import scala.collection.JavaConverters._
import scala.util.control.Breaks._

import java.text.DecimalFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.fs.{ FileStatus, Path }
import org.apache.hadoop.mapreduce._

import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.types.NumericType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{ DataFrame, SaveMode, SQLContext }
import org.apache.spark.sql.sources._
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.unsafe.types.UTF8String

import java.io.IOException
import java.io.{ ObjectInputStream, ObjectOutputStream }

import java.math.BigDecimal

import java.text.DateFormat
import java.text.DecimalFormat
import java.text.NumberFormat
import java.text.ParsePosition
import java.text.SimpleDateFormat

import java.util.Calendar
import java.util.Date
import java.util.Locale

import scala.collection.mutable.ListBuffer

import org.apache.poi.ss.util.CellAddress

import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO
import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration
import org.zuinnote.hadoop.office.format.mapreduce._
import org.zuinnote.hadoop.office.format.common.converter.ExcelConverterSimpleSpreadSheetCellDAO
import org.apache.commons.logging.LogFactory
import org.apache.commons.logging.Log
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericBooleanDataType
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericDateDataType
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericBigDecimalDataType
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericByteDataType
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericShortDataType
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericIntegerDataType
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericLongDataType
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericStringDataType
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericDataType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericDoubleDataType
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericFloatDataType

/**
 * Author: Jörn Franke <zuinnote@gmail.com>
 *
 */

/**
 *
 * Defines a Spark data source for Excel files based on hadoopoffice. It supports reading and writing of Excel files. It reads the Excelfiles into rows where each object corresponds to the SpreadSheetCellDAO. For writing, the format needs to be as well in this format or in any format and each row is written as a row in Excel according to the order of the rows in the RDD.
 *
 */

private[excel] class DefaultSource
  extends FileFormat with DataSourceRegister {
  val CONF_SIMPLEMODE="read.spark.simpleMode";
  val CONF_SIMPLEMODE_MAXROWS="read.spark.simpleMode.maxInferRows";
  val CONF_SIMPLEMODE_DATELOCALE="read.spark.simpleMode.dateLocale";
  val CONF_USEHEADER="read.spark.useHeader";
  val DEFAULT_SIMPLEMODE="false";
  val DEFAULT_USEHEADER="false";
  val DEFAULT_SIMPLEMODE_MAXROWS = "-1";
  val DEFAULT_SIMPLEMODE_DATELOCALE="US";
  
  val LOG = LogFactory.getLog(classOf[DefaultSource])
  val schema: StructType = StructType(Seq(StructField("rows", ArrayType(StructType(Seq(
    StructField("formattedValue", StringType, true),
    StructField("comment", StringType, true),
    StructField("formula", StringType, true),
    StructField("address", StringType, false),
    StructField("sheetName", StringType, false)))), true)))
  /**
   * Short alias for hadoopoffice data source.
   */
  override def shortName(): String = "excelFile"

  override def inferSchema(
    sparkSession: SparkSession,
    options:      Map[String, String],
    files:        Seq[FileStatus]): Option[StructType] = {

    // convert the Excel to a dataframe consisting of simple data types
    val simpleMode: Boolean = options.getOrElse(CONF_SIMPLEMODE,  DEFAULT_SIMPLEMODE).toBoolean
    var maxInferRows: Integer = options.getOrElse(CONF_SIMPLEMODE_MAXROWS,  DEFAULT_SIMPLEMODE_MAXROWS).toInt
    // use the first row of the Excel as header descriptors (only valid in simpleMode)
    val useHeader: Boolean = options.getOrElse(CONF_USEHEADER, DEFAULT_USEHEADER).toBoolean

    val localeBCP47: String = options.getOrElse(HadoopOfficeReadConfiguration.CONF_LOCALE.substring("hadoopoffice.".length()), "")
    val datelocaleBCP47: String = options.getOrElse(CONF_SIMPLEMODE_DATELOCALE, DEFAULT_SIMPLEMODE_DATELOCALE)
    if (!simpleMode) {

      // normal mode
      Some(schema)
    } else {
      // determine locale to interpret strings
      var locale: Locale = Locale.getDefault() // only for determining the datatype
     
      if (!"".equals(localeBCP47)) {
        locale = new Locale.Builder().setLanguageTag(localeBCP47).build()
      }
       var datelocale: Locale = Locale.getDefault()
      if (!"".equals(datelocaleBCP47)) {
        datelocale = new Locale.Builder().setLanguageTag(datelocaleBCP47).build()
      }
      val decimalFormat =  NumberFormat.getInstance(locale).asInstanceOf[DecimalFormat];
      val dateFormat = DateFormat.getDateInstance(DateFormat.SHORT, datelocale).asInstanceOf[SimpleDateFormat]
      // use the correct conf
      val broadcastedHadoopConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(new Configuration()))
      options.foreach {
        case (key, value) => broadcastedHadoopConf.value.value.set("hadoopoffice." + key, value)
      }
      // in simple mode scan through the Excel and determine the type / column
      var headers: Seq[String] = Seq()
      var defaultRow: ListBuffer[StructField] = new ListBuffer[StructField]()
      var defaultRowLength: Int = 0
      val file = files(0) // we scan only the first file
      // create a partitioned file
      val partFile = new PartitionedFile(null, file.getPath().toUri().toString(), 0, file.getLen(), Array.empty)
      val reader = new HadoopFileExcelReader(partFile, broadcastedHadoopConf.value.value)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => reader.close()))

      var i = 0
      val excelSimpleConverter = new ExcelConverterSimpleSpreadSheetCellDAO(dateFormat,decimalFormat );
      for (excelrow <- reader) {
        if ((useHeader) && (i == 0)) { // first row is the header. It is expected that it has the all columns that have data are filled
          for (x <- excelrow.get) {  
            headers = headers :+ x.asInstanceOf[SpreadSheetCellDAO].getFormattedValue
          }
          i+=1
        } else {
           if (i==maxInferRows)  break
            excelSimpleConverter.updateSpreadSheetCellRowToInferSchemaInformation(excelrow.get.asInstanceOf[Array[SpreadSheetCellDAO]])
            i += 1 // next row
        }
      }
      // create spark structtype out of the schema
      val simpleSchema = excelSimpleConverter.getSchemaRow;
      var j=0
      for (x <- simpleSchema) {
       // create column description
        var columnDescription = "c" + j.toString
              if (useHeader) {
                if (j < headers.length) {
                  columnDescription = headers(j)
                }
              }
        j +=1
        x match {
          case b: GenericBooleanDataType =>  defaultRow += StructField(columnDescription, BooleanType, true)
          case d: GenericDateDataType => defaultRow += StructField(columnDescription,DateType, true)
          case nbd: GenericBigDecimalDataType => defaultRow +=  StructField(columnDescription,DataTypes.createDecimalType(nbd.getPrecision, nbd.getScale), true)
          case nby: GenericByteDataType => defaultRow +=  StructField(columnDescription,ByteType, true)
          case ns: GenericShortDataType => defaultRow +=  StructField(columnDescription,ShortType, true)
          case ni: GenericIntegerDataType => defaultRow +=  StructField(columnDescription,IntegerType, true)
          case nl: GenericLongDataType => defaultRow += StructField(columnDescription,LongType, true)
          case s: GenericStringDataType => defaultRow +=  StructField(columnDescription,StringType, true)
          case _ => {
            LOG.warn("Unknown data type assuming string for column "+j);
            defaultRow(j) = StructField(columnDescription,StringType, true)
          }
        }
     
      }
      Some(StructType(defaultRow.toSeq))
    }

  }

  /**
   * Prepares a write job and returns an ExcelWriterFactory based on the hadoopoffice Excel input format.
   */
  override def prepareWrite(
    sparkSession: SparkSession,
    job:          Job,
    options:      Map[String, String],
    dataSchema:   StructType): OutputWriterFactory = {
    options.foreach {
      case ("mapreduce.output.fileoutputformat.compress", value) => sparkSession.conf.set("mapreduce.output.fileoutputformat.compress", value.toBoolean)
      case ("mapreduce.output.fileoutputformat.compress.codec", value) => sparkSession.conf.set("mapreduce.output.fileoutputformat.compress.codec", value)
      case (key, value) => sparkSession.conf.set("hadoopoffice." + key, value)
    }
    new ExcelOutputWriterFactory(options)
  }

  /**
   * Returns a function that can be used to read a single file in as an Iterator of InternalRow.
   *
   * @param dataSchema The global data schema. It can be either specified by the user, or
   *                   reconciled/merged from all underlying data files. If any partition columns
   *                   are contained in the files, they are preserved in this schema.
   * @param partitionSchema The schema of the partition column row that will be present in each
   *                        PartitionedFile. These columns should be appended to the rows that
   *                        are produced by the iterator.
   * @param requiredSchema The schema of the data that should be output for each row.  This may be a
   *                       subset of the columns that are present in the file if column pruning has
   *                       occurred.
   * @param filters Is ignored. The number of returned rows cannot be reduced
   * @param options A set of string -> string configuration options.
   * @return
   */
  override def buildReader(
    sparkSession:    SparkSession,
    dataSchema:      StructType,
    partitionSchema: StructType,
    requiredSchema:  StructType,
    filters:         Seq[Filter],
    options:         Map[String, String],
    hadoopConf:      Configuration): PartitionedFile => Iterator[InternalRow] = {
    var hConf=new Configuration();
      if (hadoopConf!=null) {
      hConf = hadoopConf
    } 
    val broadcastedHadoopConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hConf))
    options.foreach {
      case (key, value) => broadcastedHadoopConf.value.value.set("hadoopoffice." + key, value)
    }


    // convert the Excel to a dataframe consisting of simple data types
    val simpleMode: Boolean = options.getOrElse(CONF_SIMPLEMODE, DEFAULT_SIMPLEMODE).toBoolean
    // use the first row of the Excel as header descriptors (only valid in simpleMode)
    var useHeader: Boolean = options.getOrElse(CONF_USEHEADER, DEFAULT_USEHEADER).toBoolean
    // locale for interpreting numbers etc.
    val localeBCP47: String = options.getOrElse(HadoopOfficeReadConfiguration.CONF_LOCALE.substring("hadoopoffice.".length()), "")
    var locale: Locale = Locale.getDefault() // only for determining the datatype
    if (!"".equals(localeBCP47)) {
      locale = new Locale.Builder().setLanguageTag(localeBCP47).build()
    }
    val datelocaleBCP47: String = options.getOrElse(CONF_SIMPLEMODE_DATELOCALE, DEFAULT_SIMPLEMODE_DATELOCALE)
    var datelocale: Locale = Locale.getDefault()
      if (!"".equals(datelocaleBCP47)) {
        datelocale = new Locale.Builder().setLanguageTag(datelocaleBCP47).build()
      }
    val decimalFormat =  NumberFormat.getInstance(locale).asInstanceOf[DecimalFormat]
    val dateFormat = DateFormat.getDateInstance(DateFormat.SHORT, datelocale).asInstanceOf[SimpleDateFormat] 
    val excelSimpleConverter = new ExcelConverterSimpleSpreadSheetCellDAO(dateFormat,decimalFormat )
    // configure simpleConverter with schema
    
    val convSchema : Array[GenericDataType]  = new Array[GenericDataType](dataSchema.fields.length)
    var i=0;
    for (sf <- dataSchema.fields) {
      sf.dataType match {
        case b: BooleanType => convSchema(i)= new GenericBooleanDataType()
        case d: DateType => convSchema(i) = new GenericDateDataType()
        case nbd: DecimalType => convSchema(i) = new GenericBigDecimalDataType(nbd.precision,nbd.scale)
        case nby: ByteType => convSchema(i) = new GenericByteDataType()
        case ns: ShortType => convSchema(i) = new GenericShortDataType()
        case ni: IntegerType => convSchema(i) = new GenericIntegerDataType()
        case nl: LongType => convSchema(i) = new GenericLongDataType()
        case nd: DoubleType => convSchema(i) = new GenericDoubleDataType()
        case nf: FloatType => convSchema(i) = new GenericFloatDataType()
        case s: StringType => convSchema(i) = new GenericStringDataType()
        case _ => {
          LOG.warn("Unknown DataType in schema. Assuming String for column "+i)
          convSchema(i)=new GenericStringDataType()
        }
      }
         i+=1
    }
    
    excelSimpleConverter.setSchemaRow(convSchema)
    val broadcastConverter = sparkSession.sparkContext.broadcast(excelSimpleConverter)
    (file: PartitionedFile) => {
      val reader = new HadoopFileExcelReader(file, broadcastedHadoopConf.value.value)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => reader.close()))
      if (useHeader) {
        // skip header
        if (reader.hasNext) {
          reader.next
        }
      }
      reader.map { excelrow => // it is an arraywritable of SpreadSheetCellDAO
        {
          if (!simpleMode) { // SpreadSheetCellDAO mode
            val excelRowArray = excelrow.get
            // map the Excel row data structure to a Spark SQL schema
            val rowArray = new Array[Any](excelRowArray.length)
            var i = 0;
            for (x <- excelRowArray) { // parse through the SpreadSheetCellDAO
              if (x != null) {
                val spreadSheetCellDAOStructArray = new Array[UTF8String](5)
                val currentSpreadSheetCellDAO: SpreadSheetCellDAO = x.asInstanceOf[SpreadSheetCellDAO]
                spreadSheetCellDAOStructArray(0) = UTF8String.fromString(currentSpreadSheetCellDAO.getFormattedValue)
                spreadSheetCellDAOStructArray(1) = UTF8String.fromString(currentSpreadSheetCellDAO.getComment)
                spreadSheetCellDAOStructArray(2) = UTF8String.fromString(currentSpreadSheetCellDAO.getFormula)
                spreadSheetCellDAOStructArray(3) = UTF8String.fromString(currentSpreadSheetCellDAO.getAddress)
                spreadSheetCellDAOStructArray(4) = UTF8String.fromString(currentSpreadSheetCellDAO.getSheetName)
                // add row representing one Excel row
                rowArray(i) = InternalRow.fromSeq(spreadSheetCellDAOStructArray)
              } else {
                val spreadSheetCellDAOStructArray = new Array[UTF8String](5)
                spreadSheetCellDAOStructArray(0) = UTF8String.fromString("")
                spreadSheetCellDAOStructArray(1) = UTF8String.fromString("")
                spreadSheetCellDAOStructArray(2) = UTF8String.fromString("")
                spreadSheetCellDAOStructArray(3) = UTF8String.fromString("")
                spreadSheetCellDAOStructArray(4) = UTF8String.fromString("")
                rowArray(i) = InternalRow.fromSeq(spreadSheetCellDAOStructArray)
              }
              i += 1
            }
            val primaryRowArray: Array[Any] = new Array[Any](1)
            //primaryRowArray(0)=CatalystTypeConverters.convertToCatalyst(rowArray)
            primaryRowArray(0) = new GenericArrayData(rowArray)
            InternalRow.fromSeq(primaryRowArray)
          } else {
              // we leverage the converter
            
            val excelRowArray = excelrow.get
            val convertedRow=broadcastConverter.value.getDataAccordingToSchema(excelRowArray.asInstanceOf[Array[SpreadSheetCellDAO]])
            
            var rowData: Seq[Any] = Seq()
            // parse only the columns in required schema
            for (col <- requiredSchema.fields) {
              // find out at which position in the schema that field is
              var i = 0
              var j = 0
              for (colCandidate <- dataSchema.fields) {
                if (col.name.equals(colCandidate.name)) {
                  j = i
                }
                i += 1
              }
             
              if (j< convertedRow.length) {
                 val x = convertedRow(j)
                  
                 if (x != null)  {
                    val currentDataType = dataSchema.fields(j).dataType
                    if (currentDataType.isInstanceOf[DateType]) {
                        rowData = rowData :+ DateTimeUtils.millisToDays(x.asInstanceOf[Date].getTime())
                    } else if  (currentDataType.isInstanceOf[DecimalType]) {
                      val sparkDecimal: Decimal = new Decimal()
                      sparkDecimal.set(new scala.math.BigDecimal(x.asInstanceOf[BigDecimal]))
                      rowData = rowData :+ sparkDecimal
                    } else if (currentDataType.isInstanceOf[StringType]) {
                        rowData = rowData :+ UTF8String.fromString(x.asInstanceOf[String])
                    }    
                    else { // all other data types are "native"
                      rowData = rowData :+ x
                    }
                 } else {
                    rowData = rowData :+ null
                 }
              }
              
            
            }
          
            InternalRow.fromSeq(rowData)
          }
          

        }
      }

    }

  }
}

private[excel] class SerializableConfiguration(@transient var value: Configuration) extends Serializable {

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    value = new Configuration(false)
    value.readFields(in)
  }
}
