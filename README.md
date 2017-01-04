# spark-hadoopoffice-ds
 A [Spark datasource](http://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources) for the HadoopOffice library. This Spark datasource assumes at least Spark 2.0. Currently this datasource supports the following formats of the HadoopOffice library:

* Excel
 * Datasource format: org.zuinnote.spark.office.Excel
 * Loading and Saving of old Excel (.xls) and new Excel (.xlsx)

This datasource will be available on Spark-packages.org and on Maven Central.

Find here the status from the Continuous Integration service: https://travis-ci.org/ZuInnoTe/spark-hadoopoffice-ds/


# Release Notes

## Version 1.0.0
Version based on hadoopoffice library 1.0.0 and the new mapreduce API via the [FileFormat API](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileFormat.scala) of Spark datasources.

# Options
All [options from the HadoopOffice library](https://github.com/ZuInnoTe/hadoopoffice/wiki/Hadoop-File-Format) are supported. However, in the datasource you specify them without the prefix hadoopoffice. For example, instead of "hadoopoffice.read.locale.bcp47" you need to specify the option as "read.locale.bcp47".

Additionally, the following options of the standard Hadoop API are supported:
* mapreduce.output.fileoutputformat.compress, true if output should be compressed, false if not. Note that many office formats have already a build-in compression so an additional compression may not make sense.
* mapreduce.output.fileoutputformat.compress.codec, codec class, e.g. org.apache.hadoop.io.compress.GzipCodec



# Dependency
## Scala 2.10

groupId: com.github.zuinnote

artifactId: spark-hadoopoffice-ds_2.10

version: 1.0.0

## Scala 2.11
 
groupId: com.github.zuinnote

artifactId: spark-hadoopoffice-ds_2.11

version: 1.0.0
