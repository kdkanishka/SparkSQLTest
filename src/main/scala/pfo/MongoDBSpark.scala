package pfo

import java.util.Properties

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object MongoDBSpark extends App {

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
    .set("spark.mongodb.input.uri", "mongodb://172.17.42.1/pfo.sampleData")
    //    .set("spark.mongodb.input.collection", "sampleData")
    .setJars(Array(
    "/home/kanishka/.ivy2/cache/org.mongodb.spark/mongo-spark-connector_2.11/jars/mongo-spark-connector_2.11-2.1.5.jar",
    "/home/kanishka/.ivy2/cache/org.mongodb/mongo-java-driver/jars/mongo-java-driver-3.6.4.jar"))
  //    .set("spark.cores.max", "2")

  lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  //  val jdbcHostname = "192.168.8.102"
  //  val jdbcPort = 1433
  //  val jdbcDatabase = "PlatformOne"
  //  val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
  //
  //  val connectionProperties = new Properties()
  //  val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  //  connectionProperties.put("user", "sa")
  //  connectionProperties.put("password", "Welcome123")
  //  connectionProperties.setProperty("Driver", driverClass)

  val df = MongoSpark.load(sparkSession)
  df.printSchema()

  MongoSpark.load(sparkSession,
    ReadConfig(Map("uri" -> "mongodb://172.17.42.1/", "database" -> "pfo", "collection" -> "sampleData")
      , Some(ReadConfig(sparkSession)))).createOrReplaceTempView("ps")

  MongoSpark.load(sparkSession,
    ReadConfig(Map("uri" -> "mongodb://172.17.42.1/", "database" -> "pfo", "collection" -> "zcachetable_Edm_Responses_1_17_50_1"),
      Some(ReadConfig(sparkSession)))).createOrReplaceTempView("v1_1")

  MongoSpark.load(sparkSession,
    ReadConfig(Map("uri" -> "mongodb://172.17.42.1/", "database" -> "pfo", "collection" -> "zcachetable_Edm_Responses_1_17_50"),
      Some(ReadConfig(sparkSession)))).createOrReplaceTempView("v1")

  MongoSpark.load(sparkSession,
    ReadConfig(Map("uri" -> "mongodb://172.17.42.1/", "database" -> "pfo", "collection" -> "zcachetable_Edm_Responses_1_17_50_2"),
      Some(ReadConfig(sparkSession)))).createOrReplaceTempView("v1_2")

  MongoSpark.load(sparkSession,
    ReadConfig(Map("uri" -> "mongodb://172.17.42.1/", "database" -> "pfo", "collection" -> "zcachetable_Edm_Responses_1_17_50_3"),
      Some(ReadConfig(sparkSession)))).createOrReplaceTempView("v1_3")

  MongoSpark.load(sparkSession,
    ReadConfig(Map("uri" -> "mongodb://172.17.42.1/", "database" -> "pfo", "collection" -> "zcachetable_Edm_Responses_1_17_50_4"),
      Some(ReadConfig(sparkSession)))).createOrReplaceTempView("v2")

  MongoSpark.load(sparkSession,
    ReadConfig(Map("uri" -> "mongodb://172.17.42.1/", "database" -> "pfo", "collection" -> "zcachetable_Edm_Responses_1_17_50_5"),
      Some(ReadConfig(sparkSession)))).createOrReplaceTempView("v3")

  MongoSpark.load(sparkSession,
    ReadConfig(Map("uri" -> "mongodb://172.17.42.1/", "database" -> "pfo", "collection" -> "zcachetable_Edm_Responses_1_17_50_6"),
      Some(ReadConfig(sparkSession)))).createOrReplaceTempView("v4")

  MongoSpark.load(sparkSession,
    ReadConfig(Map("uri" -> "mongodb://172.17.42.1/", "database" -> "pfo", "collection" -> "zcachetable_Edm_Responses_1_17_50_7"),
      Some(ReadConfig(sparkSession)))).createOrReplaceTempView("v5")

  MongoSpark.load(sparkSession,
    ReadConfig(Map("uri" -> "mongodb://172.17.42.1/", "database" -> "pfo", "collection" -> "zcachetable_Edm_Sampling_1_5"),
      Some(ReadConfig(sparkSession)))).createOrReplaceTempView("sd")

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

    for (i <- 1 to 5) {
      println(s"__________________Iteration $i __________________")
      //execute query in a thread
      val ft = Future {
        val dfResult = sparkSession.sql(query)
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

