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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import org.zuinnote.hadoop.office.format.mapreduce._
   
/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

/**
*
* Defines a Spark data source for Bitcoin Blockchcain based on hadoopcryptoledgerlibrary. This is read-only for existing data. It returns BitcoinBlocks.
*
*/

class DefaultSource
  extends RelationProvider
   {

	/**
	 * Used by Spark to fetch information about the data and initiate parsing
	 *
	*
	*/
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): ExcelRelation = {
      val path= parameters.getOrElse("path", sys.error("'path' must be specified with files containing office documents"))
	// parse parameters into Hadoop parameters by adding the prefix hadoopoffice. to each key
      var hadoopParams: Map[String,String]=Map()
      parameters.foreach{ case (key,value) =>  hadoopParams+=("hadoopoffice."+key->value)}
      // create new Excel relation
      ExcelRelation(path, hadoopParams)(sqlContext)
  }

 
}
