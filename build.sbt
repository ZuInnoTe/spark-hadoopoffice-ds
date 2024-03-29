import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))
.settings(
organization := "com.github.zuinnote",
name := "spark-hadoopoffice-ds",
version := "1.7.0"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)
.enablePlugins(JacocoItPlugin)

autoScalaLibrary := false

resolvers += Resolver.mavenLocal

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))

fork  := true

crossScalaVersions := Seq("2.12.15","2.13.10")

scalacOptions += "-target:jvm-1.8"

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some(""))
}

addArtifact(artifact in (Compile, assembly), assembly)

assemblyShadeRules in assembly := Seq(
   ShadeRule.rename("org.apache.commons.compress.**" -> "hadoopoffice.shade.org.apache.commons.compress.@1").inAll
)
assemblyJarName in assembly := {
     val newName = s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar"
     newName
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)




assemblyMergeStrategy in assembly := {
  case PathList("META-INF","services",xs @ _*) => MergeStrategy.filterDistinctLines 
  case PathList("META-INF",xs @ _*) => MergeStrategy.discard  
  case _ => MergeStrategy.first
}

libraryDependencies += "com.github.zuinnote" % "hadoopoffice-fileformat" % "1.7.0" % "compile" exclude("org.apache.xmlgraphics","batik-all")

// following three libraries are only needed for digital signatures
libraryDependencies += "org.bouncycastle" % "bcprov-ext-jdk15on" % "1.70" % "compile"
libraryDependencies += "org.bouncycastle" % "bcpkix-jdk15on" % "1.70" % "compile"
libraryDependencies += "org.apache.santuario" % "xmlsec" % "3.0.0" % "compile"


libraryDependencies +=  "com.esotericsoftware" % "kryo-shaded" % "3.0.3" % "provided"


libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.2" % "provided"   exclude("org.apache.xbean","xbean-asm6-shaded")
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.2" % "provided" exclude("org.apache.xbean","xbean-asm6-shaded")

libraryDependencies += "org.apache.xbean" % "xbean-asm6-shaded" % "4.10" % "provided"  

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.14" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"

libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.19.0" % "test"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.19.0" % "it"
