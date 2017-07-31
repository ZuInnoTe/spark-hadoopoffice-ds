/**
* Copyright 2017 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
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

/**
*
* This test checks the data source
*
*/

package org.zuinnote.spark.office.excel


import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.Path

import java.io.BufferedReader
import java.io.File
import java.io.InputStream
import java.io.InputStreamReader
import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.Files
import java.nio.file.FileVisitResult
import java.nio.file.SimpleFileVisitor
import java.util.ArrayList
import java.util.List


import java.text.SimpleDateFormat

import org.apache.hadoop.io.compress.CodecPool
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.compress.Decompressor
import org.apache.hadoop.io.compress.SplittableCompressionCodec
import org.apache.hadoop.io.compress.SplitCompressionInputStream

import org.zuinnote.hadoop.office.format.common.parser.MSExcelParser
import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.util.DateTimeUtils


import scala.collection.mutable.ArrayBuffer
import org.scalatest.{FlatSpec, BeforeAndAfterAll, GivenWhenThen, Matchers}

class SparkScalaExcelDSSparkMasterIntegrationSpec extends FlatSpec with BeforeAndAfterAll with GivenWhenThen with Matchers {

private var sc: SparkContext = _


private val master: String = "local[2]"
private val appName: String = "example-scalasparkexcelinput-integrationtest"
private val tmpPrefix: String = "ho-integrationtest"
private var tmpPath: java.nio.file.Path = _
private val CLUSTERNAME: String ="hcl-minicluster"
private val DFS_INPUT_DIR_NAME: String = "/input"
private val DFS_OUTPUT_DIR_NAME: String = "/output"
private val DEFAULT_OUTPUT_FILENAME: String = "part-00000"
private val DEFAULT_OUTPUT_EXCEL_FILENAME: String = "part-00000.xlsx"
private val DFS_INPUT_DIR : Path = new Path(DFS_INPUT_DIR_NAME)
private val DFS_OUTPUT_DIR : Path = new Path(DFS_OUTPUT_DIR_NAME)
private val NOOFDATANODES: Int =4
private var dfsCluster: MiniDFSCluster = _
private var conf: Configuration = _
private var openDecompressors = ArrayBuffer[Decompressor]();

override def beforeAll(): Unit = {
    super.beforeAll()

		// Create temporary directory for HDFS base and shutdownhook
	// create temp directory
      tmpPath = Files.createTempDirectory(tmpPrefix)
      // create shutdown hook to remove temp files (=HDFS MiniCluster) after shutdown, may need to rethink to avoid many threads are created
	Runtime.getRuntime.addShutdownHook(new Thread("remove temporary directory") {
      	 override def run(): Unit =  {
        	try {
          		Files.walkFileTree(tmpPath, new SimpleFileVisitor[java.nio.file.Path]() {

            		override def visitFile(file: java.nio.file.Path,attrs: BasicFileAttributes): FileVisitResult = {
                		Files.delete(file)
             			return FileVisitResult.CONTINUE
        			}

        		override def postVisitDirectory(dir: java.nio.file.Path, e: IOException): FileVisitResult = {
          			if (e == null) {
            				Files.delete(dir)
            				return FileVisitResult.CONTINUE
          			}
          			throw e
        			}
        	})
      	} catch {
        case e: IOException => throw new RuntimeException("Error temporary files in following path could not be deleted "+tmpPath, e)
    }}})
	// create DFS mini cluster
	 conf = new Configuration()
	val baseDir = new File(tmpPath.toString()).getAbsoluteFile()
	conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath())
	val builder = new MiniDFSCluster.Builder(conf)
 	 dfsCluster = builder.numDataNodes(NOOFDATANODES).build()
	conf.set("fs.defaultFS", dfsCluster.getFileSystem().getUri().toString())
	// create local Spark cluster
 	val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
	sc = new SparkContext(sparkConf)
 }




  override def afterAll(): Unit = {
   // close Spark Context
    if (sc!=null) {
	sc.stop()
    }
    // close decompressor
	for ( currentDecompressor <- this.openDecompressors) {
		if (currentDecompressor!=null) {
			 CodecPool.returnDecompressor(currentDecompressor)
		}
 	}
    // close dfs cluster
    dfsCluster.shutdown()
    super.afterAll()
}


"The test excel file" should "be fully read with the input data source" in {
	Given("Excel 2013 test file on DFS")
  dfsCluster.getFileSystem().delete(DFS_INPUT_DIR,true)
	// create input directory
	dfsCluster.getFileSystem().mkdirs(DFS_INPUT_DIR)
	// copy test file
	val classLoader = getClass().getClassLoader()
    	// put testdata on DFS
    	val fileName: String="excel2013test.xlsx"
    	val fileNameFullLocal=classLoader.getResource(fileName).getFile()
    	val inputFile=new Path(fileNameFullLocal)
    	dfsCluster.getFileSystem().copyFromLocalFile(false, false, inputFile, DFS_INPUT_DIR)
	When("loaded by Excel data source")
  val sqlContext = new SQLContext(sc)
  val df = sqlContext.read.format("org.zuinnote.spark.office.excel").option("read.locale.bcp47", "de").load(dfsCluster.getFileSystem().getUri().toString()+DFS_INPUT_DIR_NAME)
	Then("all data can be read correctly")
	// check schema
    assert("rows"==df.columns(0))
    val rowsDF=df.select(explode(df("rows")).alias("rows"))
    assert("formattedValue"==rowsDF.select("rows.formattedValue").columns(0))
    assert("comment"==rowsDF.select("rows.comment").columns(0))
    assert("formula"==rowsDF.select("rows.formula").columns(0))
    assert("address"==rowsDF.select("rows.address").columns(0))
    assert("sheetName"==rowsDF.select("rows.sheetName").columns(0))

  // check data
  assert(6==df.count)
  val formattedValues = rowsDF.select("rows.formattedValue").collect
  val comments = rowsDF.select("rows.comment").collect
  val formulas = rowsDF.select("rows.formula").collect
  val addresses = rowsDF.select("rows.address").collect
  val sheetNames = rowsDF.select("rows.sheetName").collect
  // check first row
  assert("A1"==addresses(0).get(0))
  assert("test1"==formattedValues(0).get(0))
  assert("B1"==addresses(1).get(0))
  assert("test2"==formattedValues(1).get(0))
  assert("C1"==addresses(2).get(0))
  assert("test3"==formattedValues(2).get(0))
  assert("D1"==addresses(3).get(0))
  assert("test4"==formattedValues(3).get(0))
  // check second row
  assert("A2"==addresses(4).get(0))
  assert("4"==formattedValues(4).get(0))
  // check third row
  assert("A3"==addresses(5).get(0))
  assert("31/12/99"==formattedValues(5).get(0))
  assert("B3"==addresses(6).get(0))
  assert("5"==formattedValues(6).get(0))
  assert(""==addresses(7).get(0))
  assert(""==formattedValues(7).get(0))
  assert(""==addresses(8).get(0))
  assert(""==formattedValues(8).get(0))
  assert("E3"==addresses(9).get(0))
  assert("null"==formattedValues(9).get(0))
  // check forth row
  assert("A4"==addresses(10).get(0))
  assert("1"==formattedValues(10).get(0))
  // check fifth row
  assert("A5"==addresses(11).get(0))
  assert("2"==formattedValues(11).get(0))
  assert("B5"==addresses(12).get(0))
  assert("6"==formattedValues(12).get(0))
  assert("A5*A6"==formulas(12).get(0))
  assert("C5"==addresses(13).get(0))
  assert("10"==formattedValues(13).get(0))
  assert("A2+B5"==formulas(13).get(0))
  // check sixth row
  assert("A6"==addresses(14).get(0))
  assert("3"==formattedValues(14).get(0))
  assert("B6"==addresses(15).get(0))
  assert("4"==formattedValues(15).get(0))
  assert("C6"==addresses(16).get(0))
  assert("15"==formattedValues(16).get(0))
  assert("SUM(B3:B6)"==formulas(16).get(0))
}


"A new Excel file" should "be created on DFS and reread correctly" in {
	Given("In-memory data input")
  dfsCluster.getFileSystem().delete(DFS_OUTPUT_DIR,true)
  val sqlContext=new SQLContext(sc)
  import sqlContext.implicits._
  val sRdd = sc.parallelize(Seq(Seq("","","1","A1","Sheet1"),Seq("","This is a comment","2","A2","Sheet1"),Seq("","","3","A3","Sheet1"),Seq("","","A2+A3","B1","Sheet1"))).repartition(1)
	val df= sRdd.toDF()
	When("store as Excel file on DFS")
  df.write
      .format("org.zuinnote.spark.office.excel")
    .option("write.locale.bcp47", "de")
    .save(dfsCluster.getFileSystem().getUri().toString()+DFS_OUTPUT_DIR_NAME)
	Then("stored Excel file on DFS can be read correctly")
	// fetch results
  val dfIn = sqlContext.read.format("org.zuinnote.spark.office.excel").option("read.locale.bcp47", "de").load(dfsCluster.getFileSystem().getUri().toString()+DFS_OUTPUT_DIR_NAME)
  assert(3==dfIn.count)
  val rowsDF=dfIn.select(explode(dfIn("rows")).alias("rows"))
  val formattedValues = rowsDF.select("rows.formattedValue").collect
  val comments = rowsDF.select("rows.comment").collect
  val formulas = rowsDF.select("rows.formula").collect
  val addresses = rowsDF.select("rows.address").collect
  val sheetNames = rowsDF.select("rows.sheetName").collect
  // check data
  // check row 1
  assert("A1"==addresses(0).get(0))
  assert("1"==formattedValues(0).get(0))
  assert("B1"==addresses(1).get(0))
  assert("5"==formattedValues(1).get(0))
  assert("A2+A3"==formulas(1).get(0))
  // check row 2
  assert("A2"==addresses(2).get(0))
  assert("2"==formattedValues(2).get(0))
  assert("This is a comment"==comments(2).get(0))
  // check row 3
  assert("A3"==addresses(3).get(0))
  assert("3"==formattedValues(3).get(0))
}


"A new Excel file" should "be created on DFS with specified headers and reread" in {
Given("In-memory data input")
dfsCluster.getFileSystem().delete(DFS_OUTPUT_DIR,true)
val sqlContext=new SQLContext(sc)
import sqlContext.implicits._
val sRdd = sc.parallelize(Seq(Seq("","","1","A2","Sheet1"),Seq("","This is a comment","2","A3","Sheet1"),Seq("","","3","A4","Sheet1"),Seq("","","A3+A4","B1","Sheet1"))).repartition(1)
val columnNames = Seq("column1")

val df= sRdd.toDF(columnNames: _*)

When("store as Excel file on DFS")
df.write
    .format("org.zuinnote.spark.office.excel")
  .option("write.locale.bcp47", "de")
  .option("write.spark.useHeader",true)
  .save(dfsCluster.getFileSystem().getUri().toString()+DFS_OUTPUT_DIR_NAME)
Then("stored Excel file on DFS can be read correctly")
// fetch results
val dfIn = sqlContext.read.format("org.zuinnote.spark.office.excel").option("read.locale.bcp47", "de").load(dfsCluster.getFileSystem().getUri().toString()+DFS_OUTPUT_DIR_NAME)
assert(4==dfIn.count)
val rowsDF=dfIn.select(explode(dfIn("rows")).alias("rows"))
val formattedValues = rowsDF.select("rows.formattedValue").collect
val comments = rowsDF.select("rows.comment").collect
val formulas = rowsDF.select("rows.formula").collect
val addresses = rowsDF.select("rows.address").collect
val sheetNames = rowsDF.select("rows.sheetName").collect
// check data
// check row 1
assert("A1"==addresses(0).get(0))
assert("column1"==formattedValues(0).get(0))
assert("B1"==addresses(1).get(0))
assert("5"==formattedValues(1).get(0))
assert("A3+A4"==formulas(1).get(0))
// check row 2
assert("A2"==addresses(2).get(0))
assert("1"==formattedValues(2).get(0))
// check row 4
assert("A3"==addresses(3).get(0))
assert("2"==formattedValues(3).get(0))
assert("This is a comment"==comments(3).get(0))
// check row 5
assert("A4"==addresses(4).get(0))
assert("3"==formattedValues(4).get(0))
}

"A new Excel file" should "be created on DFS based on standard Spark datatypes and reread " in {
Given("In-memory data input")
dfsCluster.getFileSystem().delete(DFS_OUTPUT_DIR,true)
val sqlContext=new SQLContext(sc)
import sqlContext.implicits._
val df = Seq ((1000L, 2.1, "test"),(2000L,3.1,"test2")).toDF("column1","column2","column3")
When("store as Excel file on DFS")
df.repartition(1).write
    .format("org.zuinnote.spark.office.excel")
  .option("write.locale.bcp47", "de")
  .save(dfsCluster.getFileSystem().getUri().toString()+DFS_OUTPUT_DIR_NAME)

Then("stored Excel file on DFS can be read correctly")
// fetch results
val dfIn = sqlContext.read.format("org.zuinnote.spark.office.excel").option("read.locale.bcp47", "en").load(dfsCluster.getFileSystem().getUri().toString()+DFS_OUTPUT_DIR_NAME)
assert(2==dfIn.count)
val rowsDF=dfIn.select(explode(dfIn("rows")).alias("rows"))
val formattedValues = rowsDF.select("rows.formattedValue").collect
val comments = rowsDF.select("rows.comment").collect
val formulas = rowsDF.select("rows.formula").collect
val addresses = rowsDF.select("rows.address").collect
val sheetNames = rowsDF.select("rows.sheetName").collect
// check data
// check row 1
assert("A1"==addresses(0).get(0))
assert("1000"==formattedValues(0).get(0))
assert("B1"==addresses(1).get(0))
assert("2.1"==formattedValues(1).get(0))
assert("C1"==addresses(2).get(0))
assert("test"==formattedValues(2).get(0))
// check row 2
assert("A2"==addresses(3).get(0))
assert("2000"==formattedValues(3).get(0))
assert("B2"==addresses(4).get(0))
assert("3.1"==formattedValues(4).get(0))
assert("C2"==addresses(5).get(0))
assert("test2"==formattedValues(5).get(0))
}

"An existing Excel file" should "be read in a dataframe with simple datatypes" in {

dfsCluster.getFileSystem().delete(DFS_INPUT_DIR,true)
// create input directory
dfsCluster.getFileSystem().mkdirs(DFS_INPUT_DIR)
// copy test file
val classLoader = getClass().getClassLoader()
    // put testdata on DFS
    val fileName: String="testsimple.xlsx"
    val fileNameFullLocal=classLoader.getResource(fileName).getFile()
    val inputFile=new Path(fileNameFullLocal)
    dfsCluster.getFileSystem().copyFromLocalFile(false, false, inputFile, DFS_INPUT_DIR)
When("loaded by Excel data source")
val sqlContext = new SQLContext(sc)
val df = sqlContext.read.format("org.zuinnote.spark.office.excel").option("read.locale.bcp47", "de").option("read.spark.useHeader", "true").option("read.spark.simpleMode", "true").load(dfsCluster.getFileSystem().getUri().toString()+DFS_INPUT_DIR_NAME)
Then("inferred schema is correct and data is correctly parsed")
// check inferred schema decimal precision 1 scale 1, boolean, date, string, decimal precision 3 scale 3, byte, short, int, long
assert(9==df.schema.fields.length)
// check columns correctly read
assert("decimalsc1"==df.columns(0))
assert("booleancolumn"==df.columns(1))
assert("datecolumn"==df.columns(2))
assert("stringcolumn"==df.columns(3))
assert("decimalp8sc3"==df.columns(4))
assert("bytecolumn"==df.columns(5))
assert("shortcolumn"==df.columns(6))
assert("intcolumn"==df.columns(7))
assert("longcolumn"==df.columns(8))
// check data types
assert(true==df.schema.fields(0).dataType.isInstanceOf[DecimalType])
assert(2==df.schema.fields(0).dataType.asInstanceOf[DecimalType].precision)
assert(1==df.schema.fields(0).dataType.asInstanceOf[DecimalType].scale)
assert(true==df.schema.fields(1).dataType.isInstanceOf[BooleanType])
assert(true==df.schema.fields(2).dataType.isInstanceOf[DateType])
assert(true==df.schema.fields(3).dataType.isInstanceOf[StringType])
assert(true==df.schema.fields(4).dataType.isInstanceOf[DecimalType])
assert(8==df.schema.fields(4).dataType.asInstanceOf[DecimalType].precision)
assert(3==df.schema.fields(4).dataType.asInstanceOf[DecimalType].scale)
assert(true==df.schema.fields(5).dataType.isInstanceOf[ByteType])
assert(true==df.schema.fields(6).dataType.isInstanceOf[ShortType])
assert(true==df.schema.fields(7).dataType.isInstanceOf[IntegerType])
assert(true==df.schema.fields(8).dataType.isInstanceOf[LongType])
// check data
val decimalsc1 = df.select("decimalsc1").collect
val booleancolumn = df.select("booleancolumn").collect
val datecolumn = df.select("datecolumn").collect
val stringcolumn = df.select("stringcolumn").collect
val decimalp8sc3 = df.select("decimalp8sc3").collect
val bytecolumn = df.select("bytecolumn").collect
val shortcolumn = df.select("shortcolumn").collect
val intcolumn = df.select("intcolumn").collect
val longcolumn = df.select("longcolumn").collect
// check data
// check column1

assert(new java.math.BigDecimal("1.0").compareTo(decimalsc1(0).get(0).asInstanceOf[java.math.BigDecimal])==0)
assert(new java.math.BigDecimal("1.5").compareTo(decimalsc1(1).get(0).asInstanceOf[java.math.BigDecimal])==0)
assert(new java.math.BigDecimal("3.4").compareTo(decimalsc1(2).get(0).asInstanceOf[java.math.BigDecimal])==0)
assert(new java.math.BigDecimal("5.5").compareTo(decimalsc1(3).get(0).asInstanceOf[java.math.BigDecimal])==0)
assert(null==decimalsc1(4).get(0))
assert(new java.math.BigDecimal("3.4").compareTo(decimalsc1(5).get(0).asInstanceOf[java.math.BigDecimal])==0)
// check column2
assert(true==booleancolumn(0).get(0))
assert(false==booleancolumn(1).get(0))
assert(false==booleancolumn(2).get(0))
assert(false==booleancolumn(3).get(0))
assert(null==booleancolumn(4).get(0))
assert(true==booleancolumn(5).get(0))
// check column3
val sdf = new SimpleDateFormat("yyyy-MM-dd")
val expectedDate1 = sdf.parse("2017-01-01")
val expectedDate2 = sdf.parse("2017-02-28")
val expectedDate3 = sdf.parse("2000-02-29")
val expectedDate4 = sdf.parse("2017-03-01")
val expectedDate5 = null
val expectedDate6 = sdf.parse("2017-03-01")
assert(expectedDate1.compareTo(datecolumn(0).get(0).asInstanceOf[java.sql.Date])==0)
assert(expectedDate2.compareTo(datecolumn(1).get(0).asInstanceOf[java.sql.Date])==0)
assert(expectedDate3.compareTo(datecolumn(2).get(0).asInstanceOf[java.sql.Date])==0)
assert(expectedDate4.compareTo(datecolumn(3).get(0).asInstanceOf[java.sql.Date])==0)
assert(expectedDate5==datecolumn(4).get(0))
assert(expectedDate6.compareTo(datecolumn(5).get(0).asInstanceOf[java.sql.Date])==0)
// check column4

assert("This is a text"==stringcolumn(0).get(0))
assert("Another String"==stringcolumn(1).get(0))
assert("10"==stringcolumn(2).get(0))
assert("test3"==stringcolumn(3).get(0))
assert("test4"==stringcolumn(4).get(0))
assert("test5"==stringcolumn(5).get(0))
// check column5

assert(new java.math.BigDecimal("10.000").compareTo(decimalp8sc3(0).get(0).asInstanceOf[java.math.BigDecimal])==0)
assert(new java.math.BigDecimal("2.334").compareTo(decimalp8sc3(1).get(0).asInstanceOf[java.math.BigDecimal])==0)
assert(new java.math.BigDecimal("4.500").compareTo(decimalp8sc3(2).get(0).asInstanceOf[java.math.BigDecimal])==0)
assert(new java.math.BigDecimal("11.000").compareTo(decimalp8sc3(3).get(0).asInstanceOf[java.math.BigDecimal])==0)
assert(new java.math.BigDecimal("100.000").compareTo(decimalp8sc3(4).get(0).asInstanceOf[java.math.BigDecimal])==0)
assert(new java.math.BigDecimal("10000.500").compareTo(decimalp8sc3(5).get(0).asInstanceOf[java.math.BigDecimal])==0)
// check column6
assert(3==bytecolumn(0).get(0))
assert(5==bytecolumn(1).get(0))
assert(-100==bytecolumn(2).get(0))
assert(2==bytecolumn(3).get(0))
assert(3==bytecolumn(4).get(0))
assert(120==bytecolumn(5).get(0))
// check column7
assert(3==shortcolumn(0).get(0))
assert(4==shortcolumn(1).get(0))
assert(5==shortcolumn(2).get(0))
assert(250==shortcolumn(3).get(0))
assert(3==shortcolumn(4).get(0))
assert(100==shortcolumn(5).get(0))
// check column8
assert(100==intcolumn(0).get(0))
assert(65335==intcolumn(1).get(0))
assert(1==intcolumn(2).get(0))
assert(250==intcolumn(3).get(0))
assert(5==intcolumn(4).get(0))
assert(10000==intcolumn(5).get(0))
// check column9
assert(65335==longcolumn(0).get(0))
assert(1==longcolumn(1).get(0))
assert(250==longcolumn(2).get(0))
assert(10==longcolumn(3).get(0))
assert(3147483647L==longcolumn(4).get(0))
assert(10==longcolumn(5).get(0))
}


"An existing Excel file" should "be read in a dataframe with simple datatypes, written in a new file and re-read again" in {

dfsCluster.getFileSystem().delete(DFS_INPUT_DIR,true)
dfsCluster.getFileSystem().delete(DFS_OUTPUT_DIR,true)
// create input directory
dfsCluster.getFileSystem().mkdirs(DFS_INPUT_DIR)
// copy test file
val classLoader = getClass().getClassLoader()
    // put testdata on DFS
    val fileName: String="testsimple.xlsx"
    val fileNameFullLocal=classLoader.getResource(fileName).getFile()
    val inputFile=new Path(fileNameFullLocal)
    dfsCluster.getFileSystem().copyFromLocalFile(false, false, inputFile, DFS_INPUT_DIR)
When("loaded by Excel data source and written back")
val sqlContext = new SQLContext(sc)
val df = sqlContext.read.format("org.zuinnote.spark.office.excel").option("read.locale.bcp47", "de").option("read.spark.useHeader", "true").option("read.spark.simpleMode", "true").load(dfsCluster.getFileSystem().getUri().toString()+DFS_INPUT_DIR_NAME)
df.write
    .format("org.zuinnote.spark.office.excel")
  .option("write.locale.bcp47", "de")
  .option("write.spark.useHeader",true)
  .save(dfsCluster.getFileSystem().getUri().toString()+DFS_OUTPUT_DIR_NAME)
val df2=sqlContext.read.format("org.zuinnote.spark.office.excel").option("read.locale.bcp47", "de").option("read.spark.useHeader", "true").option("read.spark.simpleMode", "true").load(dfsCluster.getFileSystem().getUri().toString()+DFS_OUTPUT_DIR_NAME)
Then("inferred schema is correct and data is correctly parsed")
// check inferred schema decimal precision 1 scale 1, boolean, date, string, decimal precision 3 scale 3, byte, short, int, long
assert(9==df2.schema.fields.length)
// check columns correctly read
assert("decimalsc1"==df2.columns(0))
assert("booleancolumn"==df2.columns(1))
assert("datecolumn"==df2.columns(2))
assert("stringcolumn"==df2.columns(3))
assert("decimalp8sc3"==df2.columns(4))
assert("bytecolumn"==df2.columns(5))
assert("shortcolumn"==df2.columns(6))
assert("intcolumn"==df2.columns(7))
assert("longcolumn"==df2.columns(8))
// check data types
assert(true==df2.schema.fields(0).dataType.isInstanceOf[DecimalType])
assert(2==df2.schema.fields(0).dataType.asInstanceOf[DecimalType].precision)
assert(1==df2.schema.fields(0).dataType.asInstanceOf[DecimalType].scale)
assert(true==df2.schema.fields(1).dataType.isInstanceOf[BooleanType])
assert(true==df2.schema.fields(2).dataType.isInstanceOf[DateType])
assert(true==df2.schema.fields(3).dataType.isInstanceOf[StringType])
assert(true==df2.schema.fields(4).dataType.isInstanceOf[DecimalType])
assert(8==df2.schema.fields(4).dataType.asInstanceOf[DecimalType].precision)
assert(3==df2.schema.fields(4).dataType.asInstanceOf[DecimalType].scale)
assert(true==df2.schema.fields(5).dataType.isInstanceOf[ByteType])
assert(true==df2.schema.fields(6).dataType.isInstanceOf[ShortType])
assert(true==df2.schema.fields(7).dataType.isInstanceOf[IntegerType])
assert(true==df2.schema.fields(8).dataType.isInstanceOf[LongType])
// check data
val decimalsc1 = df2.select("decimalsc1").collect
val booleancolumn = df2.select("booleancolumn").collect
val datecolumn = df2.select("datecolumn").collect
val stringcolumn = df2.select("stringcolumn").collect
val decimalp8sc3 = df2.select("decimalp8sc3").collect
val bytecolumn = df2.select("bytecolumn").collect
val shortcolumn = df2.select("shortcolumn").collect
val intcolumn = df2.select("intcolumn").collect
val longcolumn = df2.select("longcolumn").collect
// check data
// check column1

assert(new java.math.BigDecimal("1.0").compareTo(decimalsc1(0).get(0).asInstanceOf[java.math.BigDecimal])==0)
assert(new java.math.BigDecimal("1.5").compareTo(decimalsc1(1).get(0).asInstanceOf[java.math.BigDecimal])==0)
assert(new java.math.BigDecimal("3.4").compareTo(decimalsc1(2).get(0).asInstanceOf[java.math.BigDecimal])==0)
assert(new java.math.BigDecimal("5.5").compareTo(decimalsc1(3).get(0).asInstanceOf[java.math.BigDecimal])==0)
assert(null==decimalsc1(4).get(0))
assert(new java.math.BigDecimal("3.4").compareTo(decimalsc1(5).get(0).asInstanceOf[java.math.BigDecimal])==0)
// check column2
assert(true==booleancolumn(0).get(0))
assert(false==booleancolumn(1).get(0))
assert(false==booleancolumn(2).get(0))
assert(false==booleancolumn(3).get(0))
assert(null==booleancolumn(4).get(0))
assert(true==booleancolumn(5).get(0))
// check column3
val sdf = new SimpleDateFormat("yyyy-MM-dd")
val expectedDate1 = sdf.parse("2017-01-01")
val expectedDate2 = sdf.parse("2017-02-28")
val expectedDate3 = sdf.parse("2000-02-29")
val expectedDate4 = sdf.parse("2017-03-01")
val expectedDate5 = null
val expectedDate6 = sdf.parse("2017-03-01")
assert(expectedDate1.compareTo(datecolumn(0).get(0).asInstanceOf[java.sql.Date])==0)
assert(expectedDate2.compareTo(datecolumn(1).get(0).asInstanceOf[java.sql.Date])==0)
assert(expectedDate3.compareTo(datecolumn(2).get(0).asInstanceOf[java.sql.Date])==0)
assert(expectedDate4.compareTo(datecolumn(3).get(0).asInstanceOf[java.sql.Date])==0)
assert(expectedDate5==datecolumn(4).get(0))
assert(expectedDate6.compareTo(datecolumn(5).get(0).asInstanceOf[java.sql.Date])==0)
// check column4

assert("This is a text"==stringcolumn(0).get(0))
assert("Another String"==stringcolumn(1).get(0))
assert("10"==stringcolumn(2).get(0))
assert("test3"==stringcolumn(3).get(0))
assert("test4"==stringcolumn(4).get(0))
assert("test5"==stringcolumn(5).get(0))
// check column5

assert(new java.math.BigDecimal("10.000").compareTo(decimalp8sc3(0).get(0).asInstanceOf[java.math.BigDecimal])==0)
assert(new java.math.BigDecimal("2.334").compareTo(decimalp8sc3(1).get(0).asInstanceOf[java.math.BigDecimal])==0)
assert(new java.math.BigDecimal("4.500").compareTo(decimalp8sc3(2).get(0).asInstanceOf[java.math.BigDecimal])==0)
assert(new java.math.BigDecimal("11.000").compareTo(decimalp8sc3(3).get(0).asInstanceOf[java.math.BigDecimal])==0)
assert(new java.math.BigDecimal("100.000").compareTo(decimalp8sc3(4).get(0).asInstanceOf[java.math.BigDecimal])==0)
assert(new java.math.BigDecimal("10000.500").compareTo(decimalp8sc3(5).get(0).asInstanceOf[java.math.BigDecimal])==0)
// check column6
assert(3==bytecolumn(0).get(0))
assert(5==bytecolumn(1).get(0))
assert(-100==bytecolumn(2).get(0))
assert(2==bytecolumn(3).get(0))
assert(3==bytecolumn(4).get(0))
assert(120==bytecolumn(5).get(0))
// check column7
assert(3==shortcolumn(0).get(0))
assert(4==shortcolumn(1).get(0))
assert(5==shortcolumn(2).get(0))
assert(250==shortcolumn(3).get(0))
assert(3==shortcolumn(4).get(0))
assert(100==shortcolumn(5).get(0))
// check column8
assert(100==intcolumn(0).get(0))
assert(65335==intcolumn(1).get(0))
assert(1==intcolumn(2).get(0))
assert(250==intcolumn(3).get(0))
assert(5==intcolumn(4).get(0))
assert(10000==intcolumn(5).get(0))
// check column9
assert(65335==longcolumn(0).get(0))
assert(1==longcolumn(1).get(0))
assert(250==longcolumn(2).get(0))
assert(10==longcolumn(3).get(0))
assert(3147483647L==longcolumn(4).get(0))
assert(10==longcolumn(5).get(0))
}


}
