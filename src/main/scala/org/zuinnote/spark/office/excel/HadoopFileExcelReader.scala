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

import java.io.Closeable
import java.net.URI

import org.apache.spark.sql.execution.datasources._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{ FileSplit, LineRecordReader }
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.sql.execution.datasources.RecordReaderIterator

import org.zuinnote.hadoop.office.format.mapreduce.ExcelFileInputFormat
import org.zuinnote.hadoop.office.format.mapreduce.ExcelRecordReader

import org.apache.commons.logging.LogFactory
import org.apache.commons.logging.Log

/**
 * An adaptor from a [[PartitionedFile]] to an [[Iterator]] of [[ArrayWritable<Text>]], which are all of the lines
 * in that file.
 */
class HadoopFileExcelReader(
  file: PartitionedFile, conf: Configuration) extends Iterator[ArrayWritable] with Closeable {
  val LOG = LogFactory.getLog(classOf[HadoopFileExcelReader])
  private var reader: RecordReader[Text, ArrayWritable] = null
  private val iteratorHadoop = {
    val fileSplit = new FileSplit(
      new Path(new URI(file.filePath)), file.start, file.length, Array.empty) // todo: implement locality (replace Array.empty with the locations)
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
    val inputFormat = new ExcelFileInputFormat()
    reader = inputFormat.createRecordReader(fileSplit, hadoopAttemptContext)
    reader.initialize(fileSplit, hadoopAttemptContext)
    new RecordReaderIterator(reader)
  }
  
  def getReader: RecordReader[Text, ArrayWritable] = reader

  override def hasNext: Boolean = iteratorHadoop.hasNext

  override def next(): ArrayWritable = iteratorHadoop.next()

  override def close(): Unit = {
    if (reader != null) {
      reader.close()
    }
  }
}
