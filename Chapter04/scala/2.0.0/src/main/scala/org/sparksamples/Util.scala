package org.sparksamples

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Rajdeep Dua on 2/2/16.
  */
object Util {

  val PATH = "/Users/steveyoung/ml-100k/"
  val SPARK_HOME ="/Users/steveyoung/spark-2.2.0-bin-hadoop2.7/"
  val spConfig: SparkConf = (new SparkConf).setMaster("local").setAppName("SparkApp")
  val spark: SparkSession = SparkSession
    .builder().master("local")
    .appName("Spark 2.0.0")
    //.config("spark.some.config.option", "some-value")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")
  val PATH_MOVIES = PATH + "u.item"
  val PATH_USERS = PATH + "u.user"



  def getMovieData() : RDD[String] = {
    val movie_data = sc.textFile(PATH + "u.item")
    return movie_data

  }

  def getMovieDataDF() : DataFrame = {

    //1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)
    // |0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0
    val customSchema = StructType(Array(
      StructField("id", StringType, true),
      StructField("name", StringType, true),
      StructField("date", StringType, true),
      StructField("url", StringType, true)))
    val movieDf = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "|").schema(customSchema)
      .load(PATH_MOVIES)
    movieDf
  }



  def numMovies() : Long = {
    getMovieData().count()
  }

  def movieFields() : RDD[Array[String]] = {
    this.getMovieData().map(lines =>  lines.split("\\|"))
  }

  def mean( x:Array[Int]) : Int = {
    x.sum/x.length
  }
  def getMovieAges(movie_data : RDD[String]) : scala.collection.Map[Int, Long] = {
    val movie_fields = movie_data.map(lines =>  lines.split("\\|"))
    val years = movie_fields.map( field => field(2)).map( x => convertYear(x))
    val years_filtered = years.filter(x => x != 1900)
    val movie_ages = years_filtered.map(yr =>  1998 - yr ).countByValue()
    movie_ages
  }

  def getMovieAgesDataFrame(movieData: DataFrame) : scala.collection.Map[Int, Long] = {
    /*val rowRdd_ages = sqlContext.sparkContext.textFile(PATH).map { line =>
      val tokens = line.split('|')
      Row(
        convert(
          org.sparksamples.Util.convertYear(tokens(2))
        )
      )
    }*/
    print(this.getMovieDataDF().first.getClass)

    /*val fields = Seq(
      StructField("year", IntegerType, true)
    )
    val schema = StructType(fields)
    val movies_ages = sqlContext.createDataFrame(rowRdd_ages, schema).groupBy("year").count().sort("year")
    val map = Map[Int, Long]()
    for ( x <- movies_ages ) {
      println( x )

    }*/

    null
  }



  def convertYear( x:String) : Int = {
    try
      x.takeRight(4).toInt
    catch {
      case e: Exception => println("exception caught: " + e + " Returning 1900");
        1900
    }
  }

  def getUserData : RDD[String] = {
    var user_data = Util.spark.sparkContext.textFile(PATH + "u.user")
    user_data
  }

  def getUserFields() : RDD[Array[String]] = {
    val user_data = this.getUserData
    val user_fields = user_data.map(l => l.split(","))
    return user_fields
  }

  def getUserFieldDataFrame() : DataFrame = {
    val customSchema = StructType(Array(
      StructField("no", IntegerType, true),
      StructField("age", StringType, true),
      StructField("gender", StringType, true),
      StructField("occupation", StringType, true),
      StructField("zipCode", StringType, true)));
    val spConfig = (new SparkConf).setMaster("local").setAppName("SparkApp")
    val spark = SparkSession
      .builder()
      .appName("SparkSessionZipsExample").config(spConfig)
      .getOrCreate()

    val user_df = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "|").schema(customSchema)
      .load(PATH_USERS)
    user_df
  }


  def convert(x: String) : Integer = 1998 - x.toInt
  def convert(x: Integer) : Integer = 1998 - x

}

