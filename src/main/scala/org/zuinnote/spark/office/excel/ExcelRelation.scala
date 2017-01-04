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

import scala.collection.JavaConversions._

import org.apache.spark.sql.sources.{BaseRelation,TableScan}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SQLContext 

import org.apache.spark.sql._
import org.apache.spark.rdd.RDD

import org.apache.hadoop.conf._
import org.apache.hadoop.mapreduce._



import org.apache.commons.logging.LogFactory
import org.apache.commons.logging.Log


import org.zuinnote.hadoop.office.format.common.dao._
import org.zuinnote.hadoop.office.format.mapreduce._   
import org.zuinnote.spark.office.util.ExcelFile

/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

/**
* An Excel is represented as an array containing objects of type SpreadSheetCellDAO
*
*/

case class ExcelRelation(location: String,hadoopParams: Map[String,String])
(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan
       with Serializable {
  val LOG = LogFactory.getLog(ExcelRelation.getClass);

  override def schema: StructType = {
 
      return StructType(Seq(StructField("rows",ArrayType(StructType(Seq(
						StructField("formattedValue",StringType,true),
						StructField("comment",StringType,true),
						StructField("formula",StringType,true),
						StructField("address",StringType,false),
						StructField("sheetName",StringType,false)))),true)))
    }


    /**
     * Used by Spark to fetch Excel rows according to the schema specified above from files.
     *
     *
     * returns An array with rows of the type SpreadSheetCellDAO
    **/
    override def buildScan: RDD[Row] = {
        // read ExcelRows
	val excelRowsRDD = ExcelFile.load(sqlContext, location, hadoopParams)
        // map to schema
	val schemaFields = schema.fields
        excelRowsRDD.flatMap(excelKeyValueTuple => {
		// map the Excel row data structure to a Spark SQL schema
		val rowArray = new Array[Any](excelKeyValueTuple._2.get.length)
		var i=0;
		for (x <-excelKeyValueTuple._2.get) { // parse through the SpreadSheetCellDAO
			val spreadSheetCellDAOStructArray = new Array[String](schemaFields.length)
			val currentSpreadSheetCellDAO: Array[SpreadSheetCellDAO] = excelKeyValueTuple._2.get.asInstanceOf[Array[SpreadSheetCellDAO]]
			spreadSheetCellDAOStructArray(0) = currentSpreadSheetCellDAO(i).getFormattedValue
			spreadSheetCellDAOStructArray(1) = currentSpreadSheetCellDAO(i).getComment
			spreadSheetCellDAOStructArray(2) = currentSpreadSheetCellDAO(i).getFormula
			spreadSheetCellDAOStructArray(3) = currentSpreadSheetCellDAO(i).getAddress
			spreadSheetCellDAOStructArray(4) = currentSpreadSheetCellDAO(i).getSheetName
	 		// add row representing one Excel row
			rowArray(i)=spreadSheetCellDAOStructArray
			i+=1
		}
		Some(Row.fromSeq(rowArray))
		}
        )

     }
  

}
