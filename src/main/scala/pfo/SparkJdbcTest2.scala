package pfo

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object SparkJdbcTest2 extends App {

  lazy val sparkConf = new SparkConf()
    .setAppName("Platform1 JDBC Join")
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

    val sampleTable = sparkSession.read.jdbc(jdbcUrl, "zcachetable_Edm_Sampling_1_5", connectionProperties)
  val respo1 = sparkSession.read.jdbc(jdbcUrl, "zcachetable_Edm_Responses_1_17_50", connectionProperties)
  val respo1_1 = sparkSession.read.jdbc(jdbcUrl, "zcachetable_Edm_Responses_1_17_50_1", connectionProperties)
  val respo1_2 = sparkSession.read.jdbc(jdbcUrl, "zcachetable_Edm_Responses_1_17_50_2", connectionProperties)
  val respo1_3 = sparkSession.read.jdbc(jdbcUrl, "zcachetable_Edm_Responses_1_17_50_3", connectionProperties)
  val respo2 = sparkSession.read.jdbc(jdbcUrl, "zcachetable_Edm_Responses_1_17_50_4", connectionProperties)
  val respo3 = sparkSession.read.jdbc(jdbcUrl, "zcachetable_Edm_Responses_1_17_50_5", connectionProperties)
  val respo4 = sparkSession.read.jdbc(jdbcUrl, "zcachetable_Edm_Responses_1_17_50_6", connectionProperties)
  val respo5 = sparkSession.read.jdbc(jdbcUrl, "zcachetable_Edm_Responses_1_17_50_7", connectionProperties)
  val sampleDs = sparkSession.read.jdbc(jdbcUrl, "SampleDataSet", connectionProperties)

  sampleTable.createOrReplaceTempView("sd")
  respo1.createOrReplaceTempView("v1")
  respo1_1.createOrReplaceTempView("v1_1")
  respo1_2.createOrReplaceTempView("v1_2")
  respo1_3.createOrReplaceTempView("v1_3")
  respo2.createOrReplaceTempView("v2")
  respo3.createOrReplaceTempView("v3")
  respo4.createOrReplaceTempView("v4")
  respo5.createOrReplaceTempView("v5")
  sampleDs.createOrReplaceTempView("s")

  //preload data from SampleDataSet
  //  val df3 = sampleTable.sqlContext.sql("select * from s")
  //  df3.show(20)
  //  println(">>>>>>>>> Loaded data from SampleDataSet")
  //  df3.coalesce(1).write.option("header", "true").csv("/tmp/SampleDataSet.csv")
  //  println(">>>>>>>>> Saved table to /tmp/SampleDataSet.csv")

  println(">>>>>>>>> Reading from created CSV file")
  val sampleDatasetFromCsv = sparkSession.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/home/spark/data/part-00000-5892994f-9480-4f00-a5b3-b72c7028f2bc.csv")
  println(">>>>>>>>> Done : Reading from created CSV file")
  sampleDatasetFromCsv.createOrReplaceTempView("ps")

  sampleDatasetFromCsv.printSchema()
  sampleDatasetFromCsv.show(20)

  //  sampleDs.sqlContext.setConf("spark.sql.shuffle.partitions", "5") or DISTRIBUTE BY

  val query =
    "select * from ps\n" +
      "left join v1 on ps.PanelistId = v1.PanelistId and ps.ProjectId = v1.ProjectId\n" +
      "left join v1_1 on ps.PanelistId = v1_1.PanelistId and ps.ProjectId = v1_1.ProjectId\n" +
      "left join v1_2 on ps.PanelistId = v1_2.PanelistId and ps.ProjectId = v1_2.ProjectId\n" +
      "left join v1_3 on ps.PanelistId = v1_3.PanelistId and ps.ProjectId = v1_3.ProjectId\n" +
      "left join v2 on ps.PanelistId = v2.PanelistId and ps.ProjectId = v2.ProjectId\n" +
      "left join v3 on ps.PanelistId = v3.PanelistId and ps.ProjectId = v3.ProjectId\n" +
      "left join v4 on ps.PanelistId = v4.PanelistId and ps.ProjectId = v4.ProjectId\n" +
      "left join v5 on ps.PanelistId = v5.PanelistId and ps.ProjectId = v5.ProjectId\n" +
      "left join sd on ps.PanelistId = sd.PanelistId and ps.ProjectId = sd.ProjectId\n"

//  val query2 = "select * from s"
  val t1 = System.currentTimeMillis()

  val dfResult = sparkSession.sql(query)
  dfResult.show(20)

  val t2 = System.currentTimeMillis()

//  println("Row count : " + dfResult.count())

  println(s"Elapsed time : ${t2-t1} ms")
}
