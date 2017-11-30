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

import org.apache.spark.sql.{ DataFrame, SQLContext }

import org.zuinnote.hadoop.office.format.common._
import org.zuinnote.hadoop.office.format.mapreduce._

/**
 * Author: Jörn Franke <zuinnote@gmail.com>
 *
 */

package object excelFile {

  /**
   * Adds a method, `excelFile`, to SQLContext that allows reading Excel data as rows
   */

  implicit class ExcelContext(sqlContext: SQLContext) extends Serializable {
    def excelFile(
      filePath:     String,
      hadoopParams: Map[String, String]): DataFrame = {
      val excelRelation = ExcelRelation(filePath, hadoopParams)(sqlContext)
      sqlContext.baseRelationToDataFrame(excelRelation)
    }
  }

}
