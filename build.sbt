lazy val root = (project in file(".")).
  settings(
    organization := "com.github.zuinnote",
    name := "spark-hadoopoffice-ds",
    version := "1.0.0"
  )

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))

crossScalaVersions := Seq("2.10.5", "2.11.7")

scalacOptions += "-target:jvm-1.7"

libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.7" % "provided"

libraryDependencies += "com.github.zuinnote" % "hadoopoffice-fileformat" % "1.0.0" % "compile"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.0" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.0" % "provided"

libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.4.1" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

