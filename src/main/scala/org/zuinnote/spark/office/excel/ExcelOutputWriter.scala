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

import java.math.BigDecimal
import java.sql.Date
import java.sql.Timestamp
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.sql.catalyst.{ CatalystTypeConverters, InternalRow }
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types._

import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO
import org.zuinnote.hadoop.office.format.common.util.MSExcelUtil
import org.zuinnote.hadoop.office.format.mapreduce._

import org.apache.commons.logging.LogFactory
import org.apache.commons.logging.Log
import org.zuinnote.hadoop.office.format.common.HadoopOfficeWriteConfiguration
import java.util.Locale
import java.text.DecimalFormat
import org.zuinnote.hadoop.office.format.common.converter.ExcelConverterSimpleSpreadSheetCellDAO
import java.text.NumberFormat

// NOTE: This class is instantiated and used on executor side only, no need to be serializable.
private[excel] class ExcelOutputWriter(
  path:       String,
  dataSchema: StructType,
  context:    TaskAttemptContext, options: Map[String, String]) extends OutputWriter {
  /**
   * Overrides the couple of methods responsible for generating the output streams / files so
   * that the data can be correctly partitioned
   */

  private val recordWriter: RecordWriter[NullWritable, SpreadSheetCellDAO] = new ExcelFileOutputFormat().getRecordWriter(context)
  private var currentRowNum: Int = 0;
  private val defaultSheetName: String = options.getOrElse("write.spark.defaultsheetname", "Sheet1")
  private var useHeader: Boolean = options.getOrElse("write.spark.useHeader", "false").toBoolean
  private var dateFormat: String = options.getOrElse("write.spark.dateformat", "US")
  private val localeBCP47: String = options.getOrElse(HadoopOfficeWriteConfiguration.CONF_LOCALE.substring("hadoopoffice.".length()), "")
  private var locale: Locale = Locale.getDefault() // only for determining the datatype    
      if (!"".equals(localeBCP47)) {
        locale = new Locale.Builder().setLanguageTag(localeBCP47).build()
  }
  private var converter: InternalRow => Row = _
  converter = CatalystTypeConverters.createToScalaConverter(dataSchema).asInstanceOf[InternalRow => Row]
   var datelocale: Locale = Locale.getDefault()
      if (!"".equals(dateFormat)) {
        datelocale = new Locale.Builder().setLanguageTag(dateFormat).build()
      }
  private var sdf = DateFormat.getDateInstance(DateFormat.SHORT, datelocale).asInstanceOf[SimpleDateFormat]
  private val decimalFormat = NumberFormat.getInstance(locale).asInstanceOf[DecimalFormat]
  private val simpleConverter = new ExcelConverterSimpleSpreadSheetCellDAO(sdf,decimalFormat)
/***
*
* Writes a row to Excel (Spark 2.2)
*
* The data can either be of a
* primitive type (Boolean, Byte, Short, Integer, Float, String, BigDecimal, Date,TimeStamp). In this case each value is written in the same row in Excel
* Seq of size five => All these five values are interpreted as Strings corresponding to the following fields in SpreadsheetCellDAO: formattedValue, comment, formula, address, sheetName
*
* Note: It is experimental to mix primitive type and SpreadSheetCellDAOs in one or more Rows
*
***/
  def write(row: InternalRow): Unit = {
    write(converter(row))
  }

/***
* Writes a row to Excel. Spark 2.0 and 2.1
*
* The data can either be of a
* primitive type (Boolean, Byte, Short, Integer, Float, String, BigDecimal, Date,TimeStamp). In this case each value is written in the same row in Excel
* Seq of size five => All these five values are interpreted as Strings corresponding to the following fields in SpreadsheetCellDAO: formattedValue, comment, formula, address, sheetName
*
* Note: It is experimental to mix primitive type and SpreadSheetCellDAOs in one or more Rows
*
*/
  def write(row: Row): Unit = {
    // check useHeader
    if (useHeader) {
      val headers = row.schema.fieldNames
      var i = 0
      for (x <- headers) {
        val headerColumnSCD = new SpreadSheetCellDAO(x, "", "", MSExcelUtil.getCellAddressA1Format(currentRowNum, i), defaultSheetName)
        recordWriter.write(NullWritable.get(), headerColumnSCD)
        i += 1
      }
      currentRowNum += 1
      useHeader = false
    } 
    // for each value in the row
    if (row.size>0) {
      var currentColumnNum = 0;
      val simpleObject = new Array[AnyRef](row.size)
      for (i <- 0 to row.size - 1) { // for each element of the row
        val obj = row.get(i)
        if ((obj.isInstanceOf[Seq[String]]) && (obj.asInstanceOf[Seq[String]].length==5)) {
          val formattedValue = obj.asInstanceOf[Seq[String]](0)
          val comment = obj.asInstanceOf[Seq[String]](1)
          val formula = obj.asInstanceOf[Seq[String]](2)
          val address = obj.asInstanceOf[Seq[String]](3)
          val sheetName = obj.asInstanceOf[Seq[String]](4)
          simpleObject(i) = new SpreadSheetCellDAO(formattedValue,comment,formula,address,sheetName)
        } else {
          simpleObject(i)=obj.asInstanceOf[AnyRef]
        }
      }
      // convert row to spreadsheetcellDAO
      val spreadSheetCellDAORow = simpleConverter.getSpreadSheetCellDAOfromSimpleDataType(simpleObject, defaultSheetName, currentRowNum)
      // write it
      for (x<- spreadSheetCellDAORow) {
        recordWriter.write(NullWritable.get(), x)
      }
    }
    currentRowNum += 1
  }

  override def close(): Unit = {
    recordWriter.close(context)
    currentRowNum = 0;
  }

}
