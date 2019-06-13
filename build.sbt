name := "spark-simple"

version := "0.1"

scalaVersion := "2.11.6"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.1"
libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "7.0.0.jre8"