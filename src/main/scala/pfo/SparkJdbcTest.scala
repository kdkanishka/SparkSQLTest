package pfo

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object SparkJdbcTest extends App {
  lazy val sparkConf = new SparkConf()
    .setAppName("Platform1 JDBC")
    .setMaster("spark://172.17.0.1:7077")
    .setJars(Array("/home/kanishka/.ivy2/cache/com.microsoft.sqlserver/mssql-jdbc/jars/mssql-jdbc-7.0.0.jre8.jar"))
  //    .set("spark.cores.max", "2")

  lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  val jdbcHostname = "192.168.8.102"
  val jdbcPort = 1433
  val jdbcDatabase = "PlatformOne"
  val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

  val connectionProperties = new Properties()
  val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  connectionProperties.put("user", "sa")
  connectionProperties.put("password", "Welcome123")
  connectionProperties.setProperty("Driver", driverClass)

  val sampleTable = sparkSession.read.jdbc(jdbcUrl, "SampleDataset", connectionProperties)
  sampleTable.printSchema()
  sampleTable.createOrReplaceTempView("SampleDatasetV")

//  val dfPan = sampleTable.select("PanelistId", "ProjectId", "L1", "L2").groupBy("ProjectId")
//  val df3 = sampleTable.sqlContext.sql("select PanelistId, ProjectId, L1, L2 from SampleDatasetV")
  val df3 = sampleTable.sqlContext.sql("select * from SampleDatasetV")

  df3.show(20)
}
