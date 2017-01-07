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

import java.math.BigDecimal
import java.sql.Date
import java.sql.Timestamp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types._

import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO
import org.zuinnote.hadoop.office.format.common.util.MSExcelUtil
import org.zuinnote.hadoop.office.format.mapreduce._ 


import org.apache.commons.logging.LogFactory
import org.apache.commons.logging.Log

// NOTE: This class is instantiated and used on executor side only, no need to be serializable.
private[excel] class ExcelOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext) extends OutputWriter  {
  /**
   * Overrides the couple of methods responsible for generating the output streams / files so
   * that the data can be correctly partitioned
   */
  private val recordWriter: RecordWriter[NullWritable, SpreadSheetCellDAO] = new ExcelFileOutputFormat().getRecordWriter(context)
  private var currentRowNum: Int = 0;
  private val defaultSheetName: String = context.getConfiguration().get("write.spark.defaultsheetname","Sheet1")

/***
* Writes a row to Excel.
*
* The data can either be of a 
* primitive type (Boolean, Byte, Short, Integer, Float, String, BigDecimal, Date,TimeStamp). In this case each value is written in the same row in Excel
* Seq of size five => All these five values are interpreted as Strings corresponding to the following fields in SpreadsheetCellDAO: formattedValue, comment, formula, address, sheetName
*
* Note: It is experimental to mix primitive type and SpreadSheetCellDAOs in one or more Rows
*
*/
  override def write(row: Row): Unit = {
    // for each value in the row
    var currentColumnNum=0;
    if (row.size==0) { // write empty colum
	val emptySCD = new SpreadSheetCellDAO("","","",MSExcelUtil.getCellAddressA1Format(currentRowNum,0),defaultSheetName);
        recordWriter.write(NullWritable.get(),emptySCD)
    }
    for (i <- 0 to row.size-1) { // for each element of the row
    	var x=row.get(i)
	 var formattedValue = ""
	 var comment = ""
	 var formula = ""
	 var address = ""
	 var sheetName = ""
	 x match {
  		case _: Boolean => {
			formattedValue=""
			comment=""
			formula=x.toString
			address=MSExcelUtil.getCellAddressA1Format(currentRowNum,currentColumnNum)
			sheetName=defaultSheetName
		}
 	 	case _: Byte => {
			formattedValue=""
			comment=""
			formula=x.toString
			address=MSExcelUtil.getCellAddressA1Format(currentRowNum,currentColumnNum)
			sheetName=defaultSheetName
		}
		case _: Short => {
			formattedValue=""
			comment=""
			formula=x.toString
			address=MSExcelUtil.getCellAddressA1Format(currentRowNum,currentColumnNum)
			sheetName=defaultSheetName
		}
		case _: Integer => {
			formattedValue=""
			comment=""
			formula=x.toString
			address=MSExcelUtil.getCellAddressA1Format(currentRowNum,currentColumnNum)
			sheetName=defaultSheetName
		}
		case _: Float => {
			formattedValue=""
			comment=""
			formula=x.toString
			address=MSExcelUtil.getCellAddressA1Format(currentRowNum,currentColumnNum)
			sheetName=defaultSheetName
		}
		case _: String => {
			formattedValue=x.toString
			comment=""
			formula=""
			address=MSExcelUtil.getCellAddressA1Format(currentRowNum,currentColumnNum)
			sheetName=defaultSheetName
		} 
 		case _: BigDecimal => {
			formattedValue=""
			comment=""
			formula=x.toString
			address=MSExcelUtil.getCellAddressA1Format(currentRowNum,currentColumnNum)
			sheetName=defaultSheetName
		}
		case _: Date => {
			formattedValue=x.toString
			comment=""
			formula=""
			address=MSExcelUtil.getCellAddressA1Format(currentRowNum,currentColumnNum)
			sheetName=defaultSheetName
		}
		case _: Timestamp => {
			formattedValue=x.toString
			comment=""
			formula=""
			address=MSExcelUtil.getCellAddressA1Format(currentRowNum,currentColumnNum)
			sheetName=defaultSheetName
		}
		case _: Seq[String] if (x.asInstanceOf[Seq[String]].size==5) => {
			// check if it correspond to a five String sequence (assumed to represent a SpreadSheetCellDAO).
			formattedValue=x.asInstanceOf[Seq[String]](0)
			comment=x.asInstanceOf[Seq[String]](1)
			formula=x.asInstanceOf[Seq[String]](2)
			address=x.asInstanceOf[Seq[String]](3)
			sheetName=x.asInstanceOf[Seq[String]](4)
		}
	}
	// create SpreadSheetCellDAO
	val currentSCD = new SpreadSheetCellDAO(formattedValue,comment,formula,address,sheetName)
   	recordWriter.write(NullWritable.get(),currentSCD)
	currentColumnNum+=1
    }
    currentRowNum+=1
  }

  override def close(): Unit = { 
	recordWriter.close(context)
	currentRowNum=0;
   }

}
