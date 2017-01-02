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

package org.zuinnote.spark.office.util




import org.apache.hadoop.io.Text
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext 


import org.apache.hadoop.io._
import org.apache.hadoop.conf._


import org.apache.hadoop.fs.Path

import org.zuinnote.hadoop.office.format.common.dao._
import org.zuinnote.hadoop.office.format.mapreduce._   


private[office] object ExcelFile {
 
  def load(context: SQLContext, location: String, conf: Map[String,String]): RDD[(Text,ArrayWritable)] = {
	var hadoopConf = new Configuration()	
     	conf.foreach{ case (key,value) =>  hadoopConf.set("hadoopoffice."+key,value)} 
	context.sparkContext.newAPIHadoopFile(location, classOf[ExcelFileInputFormat], classOf[Text], classOf[ArrayWritable], hadoopConf);
  }
}
