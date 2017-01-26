# spark-hadoopoffice-ds
[![Build Status](https://travis-ci.org/ZuInnoTe/spark-hadoopoffice-ds.svg?branch=master)](https://travis-ci.org/ZuInnoTe/spark-hadoopoffice-ds)

A [Spark datasource](http://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources) for the HadoopOffice library. This Spark datasource assumes at least Spark 2.0. Currently this datasource supports the following formats of the HadoopOffice library:

* Excel
 * Datasource format: org.zuinnote.spark.office.Excel
 * Loading and Saving of old Excel (.xls) and new Excel (.xlsx)

This datasource is available on [Spark-packages.org](https://spark-packages.org/package/ZuInnoTe/spark-hadoopoffice-ds) and on [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Chadoopoffice).

Find here the status from the Continuous Integration service: https://travis-ci.org/ZuInnoTe/spark-hadoopoffice-ds/


# Release Notes

## Version 1.0.1
Version based on hadoopoffice library 1.0.1 and the new mapreduce API via the [FileFormat API](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileFormat.scala) of Spark2 datasources.

# Options
All [options from the HadoopOffice library](https://github.com/ZuInnoTe/hadoopoffice/wiki/Hadoop-File-Format) are supported. However, in the datasource you specify them without the prefix hadoopoffice. For example, instead of "hadoopoffice.read.locale.bcp47" you need to specify the option as "read.locale.bcp47".

There is one option related to Spark in case you need to write rows containing primitive types. In this case a default sheetname need to be set:
* "write.spark.defaultsheetname", any valid sheetname, e.g. Sheet1

Additionally, the following options of the standard Hadoop API are supported:
* "mapreduce.output.fileoutputformat.compress", true if output should be compressed, false if not. Note that many office formats have already a build-in compression so an additional compression may not make sense.
* "mapreduce.output.fileoutputformat.compress.codec", codec class, e.g. org.apache.hadoop.io.compress.GzipCodec



# Dependency
## Scala 2.10

groupId: com.github.zuinnote

artifactId: spark-hadoopoffice-ds_2.10

version: 1.0.1

## Scala 2.11
 
groupId: com.github.zuinnote

artifactId: spark-hadoopoffice-ds_2.11

version: 1.0.1

# Schema
## Excel File
An Excel file loaded into a DataFrame  has the following schema. Basically each row contains an Array with all Excel cells in this row. For each cell the following information are available:
* formattedValue: This is what you see when you open Excel
* comment: A comment for this cell
* formula: A formula for this cell (Note: without the =, e.g. "A1+A2")
* address: The address of the cell in A1 format (e.g. "B2")
* sheetName: The name of the sheet of this cell

 ```
root                                                                                                                                                                                   
 |-- rows: array (nullable = true)                                                                                                                                                     
 |    |-- element: struct (containsNull = true)                                                                                                                                        
 |    |    |-- formattedValue: string (nullable = true)                                                                                                                                
 |    |    |-- comment: string (nullable = true)                                                                                                                                       
 |    |    |-- formula: string (nullable = true)                                                                                                                                       
 |    |    |-- address: string (nullable = true)                                                                                                                                       
 |    |    |-- sheetName: string (nullable = true)                                                                                                                          
 ```
 
# Develop
## Reading
As you can see in the schema, the datasource reads each Excel row in an array. Each element of the array is a structure describe an Excel cell. This strucutre describes the formatted value (based on the locale), the comment, the formula, the address of the cell in A1 format and the name of the sheet to which the cell belongs. In Scala you can easily read Excel files using the following snippet:

 ```
val sqlContext = sparkSession.sqlContext
val df = sqlContext.read
    .format("org.zuinnote.spark.office.excel")
    .option("read.locale.bcp47", "de")  
.load(args(0))
```
Find a full example [here](https://github.com/ZuInnoTe/hadoopoffice/wiki/Read-an-Excel-document-using-the-Spark2-datasource-API). 
## Writing
You can have two options for writing data to Excel files:
* You can have a dataframe with columns of simple datatypes (no map, no list, no struct) that should be written in rows of an Excel sheet. You can define the sheetname by using the option "write.spark.defaultsheetname" (default is "Sheet1"). In this way, you can only write values, but no formulas, comments etc.
* You can have a dataframe with arrays where each element corresponds to the schema defined above. In this case you have full control where the data ends, you can use formulas, comments etc.

The second option is illustrated in this snippet. It creates a simple Excel document with 4 cells. They are stored in sheet "Sheet1". The following Cells exist (A1 with value 1), (A2 with value 2 and comment), (A3 with value 3), (B1 with formula A2+A3). The resulting Excel file is stored in the directory /home/user/office/output
 ```
val sRdd = sparkSession.sparkContext.parallelize(Seq(Seq("","","1","A1","Sheet1"),Seq("","This is a comment","2","A2","Sheet1"),Seq("","","3","A3","Sheet1"),Seq("","","A2+A3","B1","Sheet1"))).repartition(1)
	val df= sRdd.toDF()
	df.write
      .format("org.zuinnote.spark.office.excel")
    .option("write.locale.bcp47", "de") 
.save("/home/user/office/output")
```
Find a full example [here](https://github.com/ZuInnoTe/hadoopoffice/wiki/Write-an-Excel-document-using-the-Spark2-datasource-API).
# Language bindings
## Scala
 This example loads Excel documents from the folder "/home/user/office/input" using the Excel representation (format) and shows the total number of rows, the schema and the first 20 rows. The locale for formatting cell values is set to "de". Find a full example [here](https://github.com/ZuInnoTe/hadoopoffice/wiki/Read-an-Excel-document-using-the-Spark2-datasource-API). 


 ```
val sqlContext = sparkSession.sqlContext
val df = sqlContext.read
    .format("org.zuinnote.spark.office.excel")
    .option("read.locale.bcp47", "de")  // example to set the locale to de
    .load("/home/user/office/input")
	val totalCount = df.count
	// print to screen
	println("Total number of rows in Excel: "+totalCount)	
	df.printSchema
	// print formattedValues
df.show 
```
## Java
  This example loads Excel documents from the folder "/home/user/office/input" using the Excel representation (format) and shows the total number of rows, the schema and the first 20 rows. The locale for formatting cell values is set to "de".
 ```
 SQLContext sqlContext = sparkSession.sqlContext;
 Dataframe df = sqlContext.read
 .format("org.zuinnote.spark.office.excel")
    .option("read.locale.bcp47", "de")  // example to set the locale to de
    .load("/home/user/office/input");
 	long totalCount = df.count;
	// print to screen
	System.out.println("Total number of rows in Excel: "+totalCount);
	df.printSchema();
	// print formattedValues
df.show();

```
## R
   This example loads Excel documents from the folder "/home/user/office/input" using the Excel representation (format). The locale for formatting cell values is set to "de".
```
library(SparkR)

Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.github.zuinnote:spark-hadoopoffice-ds_2.11:1.0.1" "sparkr-shell"')
sqlContext <- sparkRSQL.init(sc)

df <- read.df(sqlContext, "/home/user/office/input", source = "org.zuinnote.spark.office.excel", "read.locale.bcp47" = "de")
 ```
## Python
This example loads Excel documents from the folder "/home/user/office/input" using the Excel representation (format).The locale for formatting cell values is set to "de".
```
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.format('org.zuinnote.spark.office.excel').options('read.locale.bcp47'='de').load('/home/user/office/input')
```
## SQL
The following statement creates a table that contains Excel data in the folder //home/user/office/input. The locale for formatting cell values is set to "de".
```
CREATE TABLE ExcelData
USING  org.zuinnote.spark.office.excel
OPTIONS (path "/home/user/office/input", read.locale.bcp47 "de")
```

