package pfo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {

  lazy val sparkConf = new SparkConf()
    .setAppName("Learn Spark")
    .setMaster("spark://172.17.0.1:7077")
//    .set("spark.cores.max", "2")

  lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
}

object SparkSqlExample extends App with Context {

  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/tmp/question_tags_10K.csv")
    .toDF("id", "tag")

  dfTags.show(5)
}
