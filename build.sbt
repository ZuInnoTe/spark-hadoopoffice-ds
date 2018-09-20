import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))
.settings(
organization := "com.github.zuinnote",
name := "spark-hadoopoffice-ds",
version := "1.2.0"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)
.enablePlugins(JacocoItPlugin)


resolvers += Resolver.mavenLocal

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))

fork  := true


crossScalaVersions := Seq("2.11.12")

scalacOptions += "-target:jvm-1.8"


libraryDependencies += "com.github.zuinnote" % "hadoopoffice-fileformat" % "1.2.0" % "compile"

// following three libraries are only needed for digital signatures
libraryDependencies += "org.bouncycastle" % "bcprov-ext-jdk15on" % "1.60" % "compile"
libraryDependencies += "org.bouncycastle" % "bcpkix-jdk15on" % "1.60" % "compile"
libraryDependencies += "org.apache.santuario" % "xmlsec" % "2.1.2" % "compile"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"


libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-minicluster" % "2.7.0" % "it"
