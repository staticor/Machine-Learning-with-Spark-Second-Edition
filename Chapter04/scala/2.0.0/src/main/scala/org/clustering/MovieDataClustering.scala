package org.clustering
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}
object MovieDataClustering {


def main(args: Array[String]): Unit = {

  val PATH = "/Users/steveyoung/ml-100k/"
  val SPARK_HOME ="/Users/steveyoung/spark-2.2.0-bin-hadoop2.7/"
  val spConfig: SparkConf = (new SparkConf).setMaster("local").setAppName("SparkApp")
  val spark: SparkSession = SparkSession
    .builder().master("local")
    .appName("Spark 2.0.0")
    //.config("spark.some.config.option", "some-value")
    .getOrCreate()
  val sc = new SparkContext(config=spConfig)
  import spark.sqlContext.implicits.__

  val PATH_MOVIES = PATH + "u.item"
  val PATH_USERS = PATH + "u.user"
  val PATH_RATINGS = PATH + "u.data"
  val ratings = spark.sparkContext.textFile(PATH_RATINGS)
    .map(_.split("\t"))
    .map(lineSplit => Rating(lineSplit(0).toInt
          , lineSplit(1).toInt,
      lineSplit(2).toDouble)).toDF()
}


}
