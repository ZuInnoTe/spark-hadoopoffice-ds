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


import org.apache.hadoop.conf.Configuration
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

import org.zuinnote.hadoop.office.format.common.parser.msexcel.MSExcelParser
import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.util.DateTimeUtils


import scala.collection.mutable.ArrayBuffer
import org.scalatest.flatspec.AnyFlatSpec;
import org.scalatest._
import matchers.should._
import org.scalatest.{ BeforeAndAfterAll, GivenWhenThen }

class SparkScalaExcelDSSparkMasterIntegrationSpec extends AnyFlatSpec with BeforeAndAfterAll with BeforeAndAfterEach with GivenWhenThen with Matchers {

private var sc: SparkContext = _


private val master: String = "local[2]"
private val appName: String = "example-scalasparkexcelinput-integrationtest"
private val tmpPrefix: String = "ho-integrationtest"
private var tmpPath: java.nio.file.Path = _
private val DFS_INPUT_DIR_NAME: String = "/input"
private val DFS_OUTPUT_DIR_NAME: String = "/output"
private var INPUT_DIR_FULLNAME: String = _
private var OUTPUT_DIR_FULLNAME: String = _

private val DEFAULT_OUTPUT_FILENAME: String = "part-00000"
private val DEFAULT_OUTPUT_EXCEL_FILENAME: String = "part-00000.xlsx"
private var DFS_INPUT_DIR : String = _
private var DFS_OUTPUT_DIR : String = _
private var DFS_INPUT_DIR_PATH : Path = _
private var DFS_OUTPUT_DIR_PATH : Path = _

private var conf: Configuration = _

override def beforeAll(): Unit = {
    super.beforeAll()

		// Create temporary directory for temporary files and shutdownhook
	// create temp directory
      tmpPath = Files.createTempDirectory(tmpPrefix);
      INPUT_DIR_FULLNAME = tmpPath+DFS_INPUT_DIR_NAME;
      OUTPUT_DIR_FULLNAME = tmpPath+DFS_OUTPUT_DIR_NAME;
      DFS_INPUT_DIR = "file://"+INPUT_DIR_FULLNAME;
      DFS_OUTPUT_DIR= "file://"+OUTPUT_DIR_FULLNAME;
      DFS_INPUT_DIR_PATH = new Path(DFS_INPUT_DIR);
      DFS_OUTPUT_DIR_PATH= new Path(DFS_OUTPUT_DIR);
      val inputDirFile: File = new File(INPUT_DIR_FULLNAME);
      inputDirFile.mkdirs();
      val outputDirFile: File = new File(OUTPUT_DIR_FULLNAME);
      outputDirFile.mkdirs();
      
      // create shutdown hook to remove temp files after shutdown, may need to rethink to avoid many threads are created
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
        case e: IOException => throw new RuntimeException("Error temporary files in following path could not be deleted "+tmpPath, e);
        }
        }
  }
        )
	// create local Spark cluster
 	val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
	sc = new SparkContext(sparkConf)
 }


  override def beforeEach() {
   // clean up folders
  cleanFolder( java.nio.file.FileSystems.getDefault().getPath(INPUT_DIR_FULLNAME),false);

  cleanFolder(java.nio.file.FileSystems.getDefault().getPath(OUTPUT_DIR_FULLNAME),true);
    super.beforeEach(); // To be stackable, must call super.beforeEach
  }

  override def afterEach() {
      super.afterEach(); // To be stackable, must call super.afterEach
  }

 // clean folders
  def cleanFolder(path: java.nio.file.Path, rootFolder: Boolean) {
    if (path.toFile().exists()) {
 try {
          		Files.walkFileTree(path, new SimpleFileVisitor[java.nio.file.Path]() {

            		override def visitFile(file: java.nio.file.Path,attrs: BasicFileAttributes): FileVisitResult = {
                
                		Files.delete(file)
                  
             			return FileVisitResult.CONTINUE
        			}

        		override def postVisitDirectory(dir: java.nio.file.Path, e: IOException): FileVisitResult = {
          			if (e == null) {
                    if (rootFolder) {
                      if (dir.toFile().exists) {
            			      Files.delete(dir)
                      }
                    }
            				return FileVisitResult.CONTINUE
          			}
          			throw e
        			}
        	})
      	} catch {
        case e: IOException => throw new RuntimeException("Error temporary files in following path could not be deleted "+tmpPath, e)
    }
    }
  }

  override def afterAll(): Unit = {
   // close Spark Context
    if (sc!=null) {
	sc.stop()
    }
    super.afterAll()
}


"The test excel file" should "be fully read with the input data source" in {
	Given("Excel 2013 test file on DFS")

	// copy test file
	val classLoader = getClass().getClassLoader()
    	// put testdata on DFS
    	val fileName: String="excel2013test.xlsx"
    	val fileNameFullLocal=classLoader.getResource(fileName).getFile()


    java.nio.file.Files.copy(java.nio.file.FileSystems.getDefault().getPath(fileNameFullLocal), java.nio.file.FileSystems.getDefault().getPath(INPUT_DIR_FULLNAME+File.separator+fileName))
	When("loaded by Excel data source")
  val sqlContext = new SQLContext(sc)
  val df = sqlContext.read.format("org.zuinnote.spark.office.excel").option("read.locale.bcp47", "de").load(DFS_INPUT_DIR)
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
  val sqlContext=new SQLContext(sc)
  import sqlContext.implicits._
  val sRdd = sc.parallelize(Seq(Seq("","","1","A1","Sheet1"),Seq("","This is a comment","2","A2","Sheet1"),Seq("","","3","A3","Sheet1"),Seq("","","A2+A3","B1","Sheet1"))).repartition(1)
	val df= sRdd.toDF()
	When("store as Excel file on DFS")
  df.write
      .format("org.zuinnote.spark.office.excel")
    .option("write.locale.bcp47", "de")
    .save(DFS_OUTPUT_DIR)
	Then("stored Excel file on DFS can be read correctly")
	// fetch results
  val dfIn = sqlContext.read.format("org.zuinnote.spark.office.excel").option("read.locale.bcp47", "de").load(DFS_OUTPUT_DIR)
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
val sqlContext=new SQLContext(sc)
import sqlContext.implicits._
val sRdd = sc.parallelize(Seq(Seq("","","1","A2","Sheet1"),Seq("","This is a comment","2","A3","Sheet1"),Seq("","","3","A4","Sheet1"),Seq("","","A3+A4","B1","Sheet1"))).repartition(1)
val columnNames = Seq("column1")

val df= sRdd.toDF(columnNames: _*)

When("store as Excel file on DFS")
df.write
    .format("org.zuinnote.spark.office.excel")
  .option("write.locale.bcp47", "de")
  .option("hadoopoffice.write.header.write",true)
  .save(DFS_OUTPUT_DIR)
Then("stored Excel file on DFS can be read correctly")
// fetch results
val dfIn = sqlContext.read.format("org.zuinnote.spark.office.excel").option("read.locale.bcp47", "de").load(DFS_OUTPUT_DIR)
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
val sqlContext=new SQLContext(sc)
import sqlContext.implicits._
val df = Seq ((1000L, 2.1, "test"),(2000L,3.1,"test2")).toDF("column1","column2","column3")
When("store as Excel file on DFS")
df.repartition(1).write
    .format("org.zuinnote.spark.office.excel")
  .option("hadoopoffice.write.locale.bcp47", "de")
  .option("hadoopoffice.write.header.write",false)
  .save(DFS_OUTPUT_DIR)

Then("stored Excel file on DFS can be read correctly")
// fetch results
val dfIn = sqlContext.read.format("org.zuinnote.spark.office.excel").option("hadoopoffice.read.locale.bcp47", "en").load(DFS_OUTPUT_DIR)
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

// copy test file
val classLoader = getClass().getClassLoader()
    // put testdata on DFS
    val fileName: String="testsimple.xlsx"
    val fileNameFullLocal=classLoader.getResource(fileName).getFile()
   java.nio.file.Files.copy(java.nio.file.FileSystems.getDefault().getPath(fileNameFullLocal), java.nio.file.FileSystems.getDefault().getPath(INPUT_DIR_FULLNAME+File.separator+fileName))
	
When("loaded by Excel data source")
val sqlContext = new SQLContext(sc)
val df = sqlContext.read.format("org.zuinnote.spark.office.excel").option("hadoopoffice.read.locale.bcp47", "de").option("hadoopoffice.read.header.read", "true").option("read.spark.simplemode", "true").load(DFS_INPUT_DIR)
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
assert(3==df.schema.fields(0).dataType.asInstanceOf[DecimalType].precision)
assert(2==df.schema.fields(0).dataType.asInstanceOf[DecimalType].scale)
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

"An existing Excel file" should "be read in a dataframe without causing #25" in {

// copy test file
val classLoader = getClass().getClassLoader()
    // put testdata on DFS
    val fileName: String="sho35.xlsx"
    val fileNameFullLocal=classLoader.getResource(fileName).getFile()
   java.nio.file.Files.copy(java.nio.file.FileSystems.getDefault().getPath(fileNameFullLocal), java.nio.file.FileSystems.getDefault().getPath(INPUT_DIR_FULLNAME+File.separator+fileName))
	
When("loaded by Excel data source")
val sqlContext = new SQLContext(sc)
val df = sqlContext.read.format("org.zuinnote.spark.office.excel").option("hadoopoffice.read.locale.bcp47","us").option("hadoopoffice.read.simple.decimalformat","us").option("hadoopoffice.read.header.read", "false").option("read.spark.simplemode", "true").load(DFS_INPUT_DIR)
df.printSchema()
Then("inferred schema is correct and data is correctly parsed")
// check inferred schema decimal precision 1 scale 1, boolean, date, string, decimal precision 3 scale 3, byte, short, int, long
assert(1==df.schema.fields.length)
// check data types
assert(true==df.schema.fields(0).dataType.isInstanceOf[DecimalType])
assert(9==df.schema.fields(0).dataType.asInstanceOf[DecimalType].precision)
assert(4==df.schema.fields(0).dataType.asInstanceOf[DecimalType].scale)
// check data
val c0 = df.select("c0").collect
// check data
// check column1
assert(new java.math.BigDecimal("16884.924").compareTo(c0(0).get(0).asInstanceOf[java.math.BigDecimal])==0)
assert(new java.math.BigDecimal("1725.5523").compareTo(c0(1).get(0).asInstanceOf[java.math.BigDecimal])==0)
}

"A dataframe" should "be written in a partitioned manner" in {

// copy test file
val classLoader = getClass().getClassLoader()
    // put testdata on DFS
    val fileName: String="partitiontest.xlsx"
    val fileNameFullLocal=classLoader.getResource(fileName).getFile()
  java.nio.file.Files.copy(java.nio.file.FileSystems.getDefault().getPath(fileNameFullLocal), java.nio.file.FileSystems.getDefault().getPath(INPUT_DIR_FULLNAME+File.separator+fileName))
	
When("loaded by Excel data source")
val sqlContext = new SQLContext(sc)
val df = sqlContext.read.format("org.zuinnote.spark.office.excel").option("hadoopoffice.read.locale.bcp47", "de").option("hadoopoffice.read.header.read", "true").option("read.spark.simplemode", "true").load(DFS_INPUT_DIR)
Then("data is correctly read and written to partitioned folders")
// check size

assert(5==df.count)
// check inferred schema
assert(4==df.schema.fields.length)
// check columns correctly read
assert("Name"==df.columns(0))
assert("Year"==df.columns(1))
assert("Month"==df.columns(2))
assert("Day"==df.columns(3))
// check data types
assert(true==df.schema.fields(0).dataType.isInstanceOf[StringType])
assert(true==df.schema.fields(1).dataType.isInstanceOf[IntegerType])
assert(true==df.schema.fields(2).dataType.isInstanceOf[ByteType])
assert(true==df.schema.fields(3).dataType.isInstanceOf[ByteType])
// check data
val namecolumn = df.select("Name").collect
val yearcolumn = df.select("Year").collect
val monthcolumn = df.select("Month").collect
val daycolumn = df.select("Day").collect
assert("Test Test"==namecolumn(0).get(0))
assert("Martha MusterFrau"==namecolumn(1).get(0))
assert("Max Mustermann"==namecolumn(2).get(0))
assert("Karl Karlson"==namecolumn(3).get(0))
assert("Lennie Lennardson"==namecolumn(4).get(0))
assert(2019==yearcolumn(0).get(0))
assert(2019==yearcolumn(1).get(0))
assert(2018==yearcolumn(2).get(0))
assert(2018==yearcolumn(3).get(0))
assert(2018==yearcolumn(4).get(0))
assert(1==monthcolumn(0).get(0))
assert(1==monthcolumn(1).get(0))
assert(4==monthcolumn(2).get(0))
assert(2==monthcolumn(3).get(0))
assert(2==monthcolumn(4).get(0))
assert(12==daycolumn(0).get(0))
assert(12==daycolumn(1).get(0))
assert(7==daycolumn(2).get(0))
assert(1==daycolumn(3).get(0))
assert(1==daycolumn(4).get(0))
// write
df.write
  .option("hadoopoffice.write.locale.bcp47", "de")
  .option("hadoopoffice.write.header.write","true")
  .partitionBy("Year","Month","Day")
      .format("org.zuinnote.spark.office.excel")
 .save(DFS_OUTPUT_DIR)

// check if partitions have been created
assert(true==new java.io.File(OUTPUT_DIR_FULLNAME+"/Year=2019/Month=1/Day=12").exists())
assert(true==new java.io.File(OUTPUT_DIR_FULLNAME+"/Year=2018/Month=4/Day=7").exists())
assert(true==new java.io.File(OUTPUT_DIR_FULLNAME+"/Year=2018/Month=2/Day=1").exists())

// custom schema - this is needed due to a Spark bug (??) when reading local files that are partitioned: dataSchema and requiredSchema are empty
// check if excels with partitions can be read correctly back

val df2= sqlContext.read.format("org.zuinnote.spark.office.excel").option("hadoopoffice.read.locale.bcp47", "de").option("hadoopoffice.read.header.read", "true").option("read.spark.simplemode", "true").load(DFS_OUTPUT_DIR)
Then("data is correctly reread from partitioned folders")

assert(5==df2.count)
// check inferred schema
assert(4==df2.schema.fields.length)
// check columns correctly read
assert("Name"==df2.columns(0))
assert("Year"==df2.columns(1))
assert("Month"==df2.columns(2))
assert("Day"==df2.columns(3))
// check data types
assert(true==df2.schema.fields(0).dataType.isInstanceOf[StringType])
assert(true==df2.schema.fields(1).dataType.isInstanceOf[IntegerType])
assert(true==df2.schema.fields(2).dataType.isInstanceOf[IntegerType])
assert(true==df2.schema.fields(3).dataType.isInstanceOf[IntegerType])
// check data
val name2column = df2.select("Name").collect
val year2column = df2.select("Year").collect
val month2column = df2.select("Month").collect
val day2column = df2.select("Day").collect
assert("Test Test"==name2column(0).get(0))
assert("Martha MusterFrau"==name2column(1).get(0))
assert("Karl Karlson"==name2column(2).get(0))
assert("Lennie Lennardson"==name2column(3).get(0))
assert("Max Mustermann"==name2column(4).get(0))
assert(2019==year2column(0).get(0))
assert(2019==year2column(1).get(0))
assert(2018==year2column(2).get(0))
assert(2018==year2column(3).get(0))
assert(2018==year2column(4).get(0))
assert(1==month2column(0).get(0))
assert(1==month2column(1).get(0))
assert(2==month2column(2).get(0))
assert(2==month2column(3).get(0))
assert(4==month2column(4).get(0))
assert(12==day2column(0).get(0))
assert(12==day2column(1).get(0))
assert(1==day2column(2).get(0))
assert(1==day2column(3).get(0))
assert(7==day2column(4).get(0))
}

"An existing Excel file" should "be read in a dataframe with simple datatypes and an empty column" in {

// copy test file
val classLoader = getClass().getClassLoader()
    // put testdata on DFS
    val fileName: String="testemptycolumn.xlsx"
    val fileNameFullLocal=classLoader.getResource(fileName).getFile()
  java.nio.file.Files.copy(java.nio.file.FileSystems.getDefault().getPath(fileNameFullLocal), java.nio.file.FileSystems.getDefault().getPath(INPUT_DIR_FULLNAME+File.separator+fileName))
	
When("loaded by Excel data source")
val sqlContext = new SQLContext(sc)
val df = sqlContext.read.format("org.zuinnote.spark.office.excel").option("hadoopoffice.read.locale.bcp47", "de").option("hadoopoffice.read.header.read", "true").option("read.spark.simplemode", "true").load(DFS_INPUT_DIR)
df.printSchema()
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
assert(3==df.schema.fields(0).dataType.asInstanceOf[DecimalType].precision)
assert(2==df.schema.fields(0).dataType.asInstanceOf[DecimalType].scale)
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
// copy test file
val classLoader = getClass().getClassLoader()
    // put testdata on DFS
    val fileName: String="testsimple.xlsx"
    val fileNameFullLocal=classLoader.getResource(fileName).getFile()
  java.nio.file.Files.copy(java.nio.file.FileSystems.getDefault().getPath(fileNameFullLocal), java.nio.file.FileSystems.getDefault().getPath(INPUT_DIR_FULLNAME+File.separator+fileName))
	
When("loaded by Excel data source and written back")
val sqlContext = new SQLContext(sc)
val df = sqlContext.read.format("org.zuinnote.spark.office.excel")
.option("hadoopoffice.read.locale.bcp47", "de")
.option("hadoopoffice.read.simple.decimalformat","de")

.option("hadoopoffice.read.header.read", "true")
.option("read.spark.simpleMode", "true").load(DFS_INPUT_DIR)
df.printSchema
df.write
    .format("org.zuinnote.spark.office.excel")
  .option("hadoopoffice.write.locale.bcp47", "de")
  .option("hadoopoffice.write.header.write",true)
  .save(DFS_OUTPUT_DIR)
val df2=sqlContext.read.format("org.zuinnote.spark.office.excel")
.option("hadoopoffice.read.locale.bcp47", "de")
.option("hadoopoffice.read.header.read", "true")
.option("read.spark.simplemode", "true").load(DFS_OUTPUT_DIR)
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
