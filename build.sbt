import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))
.settings(
organization := "com.github.zuinnote",
name := "spark-hadoopoffice-ds",
version := "1.0.2"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)



resolvers += Resolver.mavenLocal

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))

fork  := true

jacoco.settings

itJacoco.settings


crossScalaVersions := Seq("2.10.5", "2.11.7")

scalacOptions += "-target:jvm-1.7"


libraryDependencies += "com.github.zuinnote" % "hadoopoffice-fileformat" % "1.0.2" % "compile"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"


libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-minicluster" % "2.7.0" % "it"
