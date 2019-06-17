package pfo

package pfo

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object SparkCsvDataProcessing extends App {

  def readCsv(file: String, tempView: String) = {
    println(">>>>>>>>> Reading from created CSV file")
    val sampleDatasetFromCsv = sparkSession.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(file)
    println(">>>>>>>>> Done : Reading from created CSV file")
    sampleDatasetFromCsv.createOrReplaceTempView(tempView)

    sampleDatasetFromCsv.printSchema()
    sampleDatasetFromCsv.show(5)
    sampleDatasetFromCsv
  }

  lazy val sparkConf = new SparkConf()
    .setAppName("Platform1 JDBC Join")
    .setMaster("spark://172.17.42.1:7077")
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

  val sampleTable = readCsv("/home/spark/data/zcachetable_Edm_Sampling_1_5_201906112353.csv", "sd")
  val respo1 = readCsv("/home/spark/data/zcachetable_Edm_Responses_1_17_50_201906112355.csv", "v1")
  val respo1_1 = readCsv("/home/spark/data/zcachetable_Edm_Responses_1_17_50_1_201906112355.csv", "v1_1")
  val respo1_2 = readCsv("/home/spark/data/zcachetable_Edm_Responses_1_17_50_2_201906112356.csv", "v1_2")
  val respo1_3 = readCsv("/home/spark/data/zcachetable_Edm_Responses_1_17_50_3_201906112356.csv", "v1_3")
  val respo2 = readCsv("/home/spark/data/zcachetable_Edm_Responses_1_17_50_4_201906112358.csv", "v2")
  val respo3 = readCsv("/home/spark/data/zcachetable_Edm_Responses_1_17_50_5_201906112358.csv", "v3")
  val respo4 = readCsv("/home/spark/data/zcachetable_Edm_Responses_1_17_50_6_201906112359.csv", "v4")
  val respo5 = readCsv("/home/spark/data/zcachetable_Edm_Responses_1_17_50_7_201906112359.csv", "v5")
  val sampleDs = readCsv("/home/spark/data/part-00000-5892994f-9480-4f00-a5b3-b72c7028f2bc.csv", "ps")

  //preload data from SampleDataSet
  //  val df3 = sampleTable.sqlContext.sql("select * from s")
  //  df3.show(20)
  //  println(">>>>>>>>> Loaded data from SampleDataSet")
  //  df3.coalesce(1).write.option("header", "true").csv("/tmp/SampleDataSet.csv")
  //  println(">>>>>>>>> Saved table to /tmp/SampleDataSet.csv")

  //  println(">>>>>>>>> Reading from created CSV file")
  //  val savedFile = sparkSession.read.format("csv")
  //    .option("header", "true")
  //    .option("inferSchema", "true")
  //    .load("/home/spark/data/part-00000-5892994f-9480-4f00-a5b3-b72c7028f2bc.csv")
  //  println(">>>>>>>>> Done : Reading from created CSV file")
  //  savedFile.createOrReplaceTempView("ps")

  //  savedFile.printSchema()
  //  savedFile.show(20)

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

  var lf = List[Future[Any]]()

  for (i <- 1 to 2) {
    println(s"__________________Iteration $i __________________")
    //execute query in a thread
    val ft = Future {
//      val dfResult = sparkSession.sql(query)

      //pivot example
      val dfResult = sparkSession.sql(query).groupBy("ps.PanelistId").pivot("ps.ProjectId").avg("ps.L4")
      dfResult.show(20)
    }
    println(s"____________________Done $i ____________________")
    lf = lf :+ ft
  }
  //  println("Row count : " + dfResult.count())

  for (f <- lf) {
    Await.result(f, 1000 second)
  }

  val t2 = System.currentTimeMillis()

  println(s"Elapsed time : ${t2 - t1} ms")
}
