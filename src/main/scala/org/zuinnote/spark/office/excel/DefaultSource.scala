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
**/
package org.zuinnote.spark.office.excel



import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.fs.{FileStatus, Path}
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
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.unsafe.types.UTF8String




import java.io.IOException
import java.io.{ObjectInputStream, ObjectOutputStream}

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


import org.apache.commons.logging.LogFactory
import org.apache.commons.logging.Log

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
  extends FileFormat with DataSourceRegister
   {
   val LOG = LogFactory.getLog(classOf[DefaultSource])
val schema : StructType = StructType(Seq(StructField("rows",ArrayType(StructType(Seq(
						StructField("formattedValue",StringType,true),
						StructField("comment",StringType,true),
						StructField("formula",StringType,true),
						StructField("address",StringType,false),
						StructField("sheetName",StringType,false)
				))),true)))
  /**
   * Short alias for hadoopcryptoledger BitcoinBlock data source.
   */
  override def shortName(): String = "excelFile"


  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
files: Seq[FileStatus]): Option[StructType] = {

      // convert the Excel to a dataframe consisting of simple data types
      val simpleMode: Boolean = options.getOrElse("read.spark.simpleMode","false").toBoolean
      // use the first row of the Excel as header descriptors (only valid in simpleMode)
      val useHeader: Boolean = options.getOrElse("read.spark.useHeader","false").toBoolean
      val localeBCP47: String = options.getOrElse(HadoopOfficeReadConfiguration.CONF_LOCALE.substring("hadoopoffice.".length()),"")

    if (!simpleMode) {

    // normal mode
		Some(schema)
    } else {
      // determine locale to interpret strings
      var locale: Locale = Locale.getDefault() // only for determining the datatype
      if (!"".equals(localeBCP47)) {
          locale = new Locale.Builder().setLanguageTag(localeBCP47).build()
      }
      // use the correct conf
      val broadcastedHadoopConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(new Configuration()))
          options.foreach{
    			case (key,value) => broadcastedHadoopConf.value.value.set("hadoopoffice."+key,value)
          }
      // in simple mode scan through the Excel and determine the type / column
      var headers: Seq[String] = Seq()
      var defaultRow: ListBuffer[StructField] = new ListBuffer[StructField]()
      var defaultRowLength: Int = 0
      val file = files (0) // we scan only the first file
      // create a partitioned file
      val partFile = new PartitionedFile(null, file.getPath().toUri().toString(), 0, file.getLen(), Array.empty)
      val reader = new HadoopFileExcelReader(partFile, broadcastedHadoopConf.value.value)
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => reader.close()))

     var i = 0
     for (excelrow <- reader) {



        val excelRowArray = excelrow.get
        if (excelRowArray.length>defaultRowLength) { // maybe a row is larger than we thought (e.g. if there are null values in the beginning)
          defaultRowLength=excelRowArray.length
        }
        var j=0
        for (x <-excelRowArray) { // parse through the SpreadSheetCellDAO
        var dataTypeFound=false
        if ((useHeader) && (i==0)) { // first row is the header. It is expected that it has the all columns that have data are filled
            headers=headers:+x.asInstanceOf[SpreadSheetCellDAO].getFormattedValue
        } else
          if (x!=null) {
            val currentSpreadSheetCellDAO: SpreadSheetCellDAO=x.asInstanceOf[SpreadSheetCellDAO]
            j=new CellAddress(currentSpreadSheetCellDAO.getAddress()).getColumn()
            if (j>=defaultRow.length) {
                // fill up
                for (x <- defaultRow.length to j) {
                    defaultRow+=null
                }
            }
            // assumption: empty strings are null values
              if (!("".equals(currentSpreadSheetCellDAO.getFormattedValue))) {
              // determine column description
              var columnDescription = "c"+j.toString
              if (useHeader) {
                  if (j<headers.length) {
                    columnDescription = headers(j)
                  }
              }
              val currentCellValue= currentSpreadSheetCellDAO.getFormattedValue
            //  check if boolean
              if (("TRUE".equals(currentCellValue))||("FALSE".equals(currentCellValue))) {
                dataTypeFound=true
                if (defaultRow(j)!=null) { // check if previous assumption was boolean
                       val currentField = defaultRow(j)
                       if (!(currentField.dataType.isInstanceOf[BooleanType])) {
                           // if not then the type needs to be set to string
                          defaultRow(j)=StructField(columnDescription,StringType,true)
                       }
                      // if yes then nothing todo (already boolean)

                } else { // we face this the first time
                    defaultRow(j)=StructField(columnDescription,BooleanType,true)
                }
              }
            // check if date (note currently POI returns mm/dd/yyyy (US-Default))
            // in the future POI (might) change this, for which we an easily adapt by using another locale below
            val dateFormat = DateFormat.getDateInstance(DateFormat.SHORT,Locale.US)
            if ((!dataTypeFound) && (dateFormat.isInstanceOf[SimpleDateFormat])) {

              val sdf = dateFormat.asInstanceOf[SimpleDateFormat]
              val theDate = sdf.parse(currentCellValue, new ParsePosition(0))
              if (theDate!=null) { // we have indeed a date

              dataTypeFound=true
                  if (defaultRow(j)!=null) { // check if previous assumption was date
                    val currentField = defaultRow(j)
                    if (!(currentField.dataType.isInstanceOf[DateType])) {
                      // if not then the type needs to be set to string
                     defaultRow(j)=StructField(columnDescription,StringType,true)
                    }
                  }else { // we face this the first time
                      defaultRow(j)=StructField(columnDescription,DateType,true)
                  }
              }
            }

            // check if BigDecimal
            val dForm = NumberFormat.getInstance(locale)
            dForm.asInstanceOf[DecimalFormat].setParseBigDecimal(true)
            val bd  =  dForm.asInstanceOf[DecimalFormat].parse(currentCellValue,new ParsePosition(0))
            if ((!dataTypeFound) && (bd!=null)) {
              val bdv : BigDecimal = bd.asInstanceOf[BigDecimal].stripTrailingZeros()

              dataTypeFound=true

              if (defaultRow(j)!=null) { // check if previous assumption was a number
                     val currentField = defaultRow(j)
                     // check if we need to upgrade to decimal
                     if ((bdv.scale>0) && (currentField.dataType.isInstanceOf[NumericType])) {
                              // upgrade to decimal, if necessary
                              if (!(currentField.dataType.isInstanceOf[DecimalType])) {
                                  defaultRow(j)=StructField(columnDescription,DataTypes.createDecimalType(bdv.precision,bdv.scale),true)
                              } else {
                                if ((bdv.scale > currentField.dataType.asInstanceOf[DecimalType].scale) && (bdv.precision>currentField.dataType.asInstanceOf[DecimalType].precision)){
                                  defaultRow(j)=StructField(columnDescription,DataTypes.createDecimalType(bdv.precision,bdv.scale),true)
                                } else if (bdv.scale > currentField.dataType.asInstanceOf[DecimalType].scale) {
                                      // upgrade scale
                                      defaultRow(j)=StructField(columnDescription,DataTypes.createDecimalType(currentField.dataType.asInstanceOf[DecimalType].precision,bdv.scale),true)
                                } else if (bdv.precision>currentField.dataType.asInstanceOf[DecimalType].precision) {
                                  // upgrade precision
                                  // new precision is needed to extend to max scale
                                  val newpre = bdv.precision+(currentField.dataType.asInstanceOf[DecimalType].scale-bdv.scale)
                                  defaultRow(j)=StructField(columnDescription,DataTypes.createDecimalType(newpre,currentField.dataType.asInstanceOf[DecimalType].scale),true)
                                }
                              }
                      } else { // check if we need to upgrade one of the integer types
                           // if current is byte
                           var isByte=false
                           var isShort=false
                           var isInt=false
                           var isLong=true
                           try {
                                bdv.longValueExact()
                                isLong=true
                                bdv.intValueExact()
                                isInt=true
                                bdv.shortValueExact()
                                isShort=true
                                bdv.byteValueExact()
                                isByte=true
                           } catch{

                             case e: Exception => LOG.debug("Possible data types: Long: "+isLong+" Int: "+isInt+" Short: "+isShort+" Byte: "+isByte)
                           }
                           // if it was Numeric before we can ignore testing the byte case, here just for completeness
                           if ((isByte) && ((currentField.dataType.isInstanceOf[ByteType]) || (currentField.dataType.isInstanceOf[ShortType])|| (currentField.dataType.isInstanceOf[IntegerType])|| (currentField.dataType.isInstanceOf[LongType]))) {
                            // if it was Byte before we can ignore testing the byte case, here just for completeness
                           } else
                           if ((isShort) && ((currentField.dataType.isInstanceOf[ByteType]))) {
                              // upgrade to short
                              defaultRow(j)=StructField(columnDescription,DataTypes.ShortType,true)
                           } else
                           if ((isInt) && ((currentField.dataType.isInstanceOf[ShortType])|| (currentField.dataType.isInstanceOf[ByteType]))) {
                              // upgrade to integer
                              defaultRow(j)=StructField(columnDescription,DataTypes.IntegerType,true)
                           } else
                           if ((!isByte) && (!isShort) && (!isInt) && !((currentField.dataType.isInstanceOf[LongType]))) {
                              // upgrade to integer
                              defaultRow(j)=StructField(columnDescription,DataTypes.LongType,true)
                           }


                      }

                     } else {
                        // we face it for the first time
                        // determine value type
                        if (bdv.scale>0) {
                          defaultRow(j)=StructField(columnDescription,DataTypes.createDecimalType(bdv.precision,bdv.scale),true)
                        } else {
                          var isByte=false
                          var isShort=false
                          var isInt=false
                          var isLong=true
                          try {
                               bdv.longValueExact()
                               isLong=true
                               bdv.intValueExact()
                               isInt=true
                               bdv.shortValueExact()
                               isShort=true
                               bdv.byteValueExact()
                               isByte=true
                          } catch{

                            case e: Exception => LOG.debug("Possible data types: Long: "+isLong+" Int: "+isInt+" Short: "+isShort+" Byte: "+isByte)
                          }
                          if (isByte) {
                            defaultRow(j)=StructField(columnDescription,DataTypes.ByteType,true)
                          } else if (isShort) {
                            defaultRow(j)=StructField(columnDescription,DataTypes.ShortType,true)
                          } else if (isInt) {
                            defaultRow(j)=StructField(columnDescription,DataTypes.IntegerType,true)
                          } else if (isLong) {
                            defaultRow(j)=StructField(columnDescription,DataTypes.LongType,true)
                          }
                        }
                     }
                   }
                   if (!dataTypeFound) {
                   // otherwise string
                     if (defaultRow.length>j) {
                       defaultRow(j)=StructField(columnDescription,StringType,true)
                     }  else { // we face this the first time
                         defaultRow(j)=StructField(columnDescription,StringType,true)
                     }
                    }



          } else { // ignore nulls for schema

          }

        }


      }
      i+=1 // next row

      }

      Some(StructType(defaultRow.toSeq))
	}

  }





  /**
   * Prepares a write job and returns an ExcelWriterFactory based on the hadoopoffice Excel input format.
   */
  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
dataSchema: StructType): OutputWriterFactory = {
options.foreach{
   case("mapreduce.output.fileoutputformat.compress",value) => sparkSession.conf.set("mapreduce.output.fileoutputformat.compress",value.toBoolean)
   case("mapreduce.output.fileoutputformat.compress.codec",value) => sparkSession.conf.set("mapreduce.output.fileoutputformat.compress.codec",value)
   case (key,value) => sparkSession.conf.set("hadoopoffice."+key,value)
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
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
	val broadcastedHadoopConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
      options.foreach{
			case (key,value) => broadcastedHadoopConf.value.value.set("hadoopoffice."+key,value)
      }
      // convert the Excel to a dataframe consisting of simple data types
      val simpleMode: Boolean = options.getOrElse("read.spark.simpleMode","false").toBoolean
      // use the first row of the Excel as header descriptors (only valid in simpleMode)
      var useHeader: Boolean = options.getOrElse("read.spark.useHeader","false").toBoolean
      // locale for interpreting numbers etc.
      val localeBCP47: String = options.getOrElse(HadoopOfficeReadConfiguration.CONF_LOCALE.substring("hadoopoffice.".length()),"")
      var locale: Locale = Locale.getDefault() // only for determining the datatype
      if (!"".equals(localeBCP47)) {
          locale = new Locale.Builder().setLanguageTag(localeBCP47).build()
      }
      (file: PartitionedFile) => {
		  val reader = new HadoopFileExcelReader(file, broadcastedHadoopConf.value.value)
		 Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => reader.close()))
     if ((simpleMode) && (useHeader)) {
        // skip header
        if (reader.hasNext) {
          reader.next
        }
     }
		reader.map { excelrow => // it is an arraywritable of SpreadSheetCellDAO
			{ if (!simpleMode) { // SpreadSheetCellDAO mode
    			val excelRowArray = excelrow.get
    			// map the Excel row data structure to a Spark SQL schema
    			val rowArray = new Array[Any](excelRowArray.length)
    			var i=0;
    			for (x <-excelRowArray) { // parse through the SpreadSheetCellDAO
    				if (x!=null) {
    					val spreadSheetCellDAOStructArray = new Array[UTF8String](5)
    					val currentSpreadSheetCellDAO: SpreadSheetCellDAO=x.asInstanceOf[SpreadSheetCellDAO]
    					spreadSheetCellDAOStructArray(0) = UTF8String.fromString(currentSpreadSheetCellDAO.getFormattedValue)
    					spreadSheetCellDAOStructArray(1) = UTF8String.fromString(currentSpreadSheetCellDAO.getComment)
    					spreadSheetCellDAOStructArray(2) = UTF8String.fromString(currentSpreadSheetCellDAO.getFormula)
    					spreadSheetCellDAOStructArray(3) = UTF8String.fromString(currentSpreadSheetCellDAO.getAddress)
    					spreadSheetCellDAOStructArray(4) =UTF8String.fromString(currentSpreadSheetCellDAO.getSheetName)
    		 			// add row representing one Excel row
    					rowArray(i)=InternalRow.fromSeq(spreadSheetCellDAOStructArray)
    				} else {
    					val spreadSheetCellDAOStructArray = new Array[UTF8String](5)
    					spreadSheetCellDAOStructArray(0) = UTF8String.fromString("")
    					spreadSheetCellDAOStructArray(1) = UTF8String.fromString("")
    					spreadSheetCellDAOStructArray(2) = UTF8String.fromString("")
    					spreadSheetCellDAOStructArray(3) = UTF8String.fromString("")
    					spreadSheetCellDAOStructArray(4) = UTF8String.fromString("")
    					rowArray(i)=InternalRow.fromSeq(spreadSheetCellDAOStructArray)
    				}
    				i+=1
    			}
    			val primaryRowArray: Array[Any] = new Array[Any](1)
    			//primaryRowArray(0)=CatalystTypeConverters.convertToCatalyst(rowArray)
    			primaryRowArray(0)=new GenericArrayData(rowArray)
    			InternalRow.fromSeq(primaryRowArray)
      }   else {

            val excelRowArray = excelrow.get
            // get datatype from schema
            //dataSchema
            //dataschema.
            var rowData: Seq[Any] = Seq()
            var j = 0;
            for (x <-excelRowArray) { // parse through the SpreadSheetCellDAO
              if (x!=null) {
                  val currentCellValue = x.asInstanceOf[SpreadSheetCellDAO].getFormattedValue
                  val currentDataType=dataSchema.fields(j).dataType

                  if ("".equals(currentCellValue)) {
                      rowData=rowData:+null
                  } else if (currentDataType.isInstanceOf[BooleanType]) {
                      rowData=rowData:+currentCellValue.toBoolean
                  } else if (currentDataType.isInstanceOf[DateType]) {
                      val dateFormat = DateFormat.getDateInstance(DateFormat.SHORT,Locale.US)
                      val sdf = dateFormat.asInstanceOf[SimpleDateFormat]
                      val theDate = sdf.parse(currentCellValue, new ParsePosition(0))
                      if (theDate!=null) {

                        rowData=rowData:+DateTimeUtils.millisToDays(theDate.getTime())
                      } else {
                          rowData=rowData:+null
                      }
                  } else if (currentDataType.isInstanceOf[NumericType]) {

                    val dForm = NumberFormat.getInstance(locale)
                    dForm.asInstanceOf[DecimalFormat].setParseBigDecimal(true)
                    val bd  =  dForm.asInstanceOf[DecimalFormat].parse(currentCellValue,new ParsePosition(0))
                    if (bd!=null) {
                        val bdv : BigDecimal = bd.asInstanceOf[BigDecimal].stripTrailingZeros()
                      if (currentDataType.isInstanceOf[ByteType]) {
                        rowData=rowData:+bdv.byteValueExact
                      } else if (currentDataType.isInstanceOf[ShortType]) {
                        rowData=rowData:+bdv.shortValueExact
                      } else if (currentDataType.isInstanceOf[IntegerType]) {
                        rowData=rowData:+bdv.intValueExact
                      } else if (currentDataType.isInstanceOf[LongType]) {
                        rowData=rowData:+bdv.longValueExact
                      } else if (currentDataType.isInstanceOf[DecimalType]) {
                          val currentDT = currentDataType.asInstanceOf[DecimalType]
                          val sparkDecimal: Decimal = new Decimal()
                          sparkDecimal.set(new scala.math.BigDecimal(bdv))
                          rowData=rowData:+sparkDecimal
                        }
                      } else {
                        rowData=rowData:+null
                      }

                  }else if (currentDataType.isInstanceOf[StringType]) {
                    rowData=rowData:+UTF8String.fromString(currentCellValue)
                  }  else {
                      throw new InterruptedException("Do not know how to handle datatype")
                  }
      		     } else {
                //
                rowData=rowData:+null
               }
               j+=1
            }


            InternalRow.fromSeq(rowData)
          }

      }

		}
      }

  }





}

private[excel]
class SerializableConfiguration(@transient var value: Configuration) extends Serializable {

  private def writeObject(out: ObjectOutputStream): Unit =  {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit =  {
    value = new Configuration(false)
    value.readFields(in)
  }
}
