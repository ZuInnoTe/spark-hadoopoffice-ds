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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.ArrayType
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


import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO
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
		Some(schema)
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
      (file: PartitionedFile) => {
		  val reader = new HadoopFileExcelReader(file, broadcastedHadoopConf.value.value)
		 Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => reader.close()))
		reader.map { excelrow => // it is an arraywritable of SpreadSheetCellDAO
			{
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
