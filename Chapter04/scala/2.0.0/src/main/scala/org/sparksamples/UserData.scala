<<<<<<< HEAD
//
=======
package org.sparksamples
>>>>>>> 40cc251d08389a87f1400e35a96a482d20b1ad86
//import org.apache.spark.sql.SQLContext
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
/**
  * Created by Rajdeep Dua on 8/22/16.
  */
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType};
object UserData {
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val customSchema = StructType(Array(
      StructField("no", IntegerType, true),
      StructField("age", StringType, true),
      StructField("gender", StringType, true),
      StructField("occupation", StringType, true),
      StructField("zipCode", StringType, true)));
    val spConfig = (new SparkConf).setMaster("local").setAppName("SparkApp")
    val spark = SparkSession
      .builder()
      .appName("SparkUserData").config(spConfig)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val user_df = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "|").schema(customSchema)
<<<<<<< HEAD
      .load("/Users/steveyoung/ml-100k/u.user")
=======
      .load("/Users/steve/ml-100k/u.user")
>>>>>>> 40cc251d08389a87f1400e35a96a482d20b1ad86
    val first = user_df.first()
    println("First Record : " + first)

    val num_genders = user_df.groupBy("gender").count().count()
    val num_occupations = user_df.groupBy("occupation").count().count()
    val num_zipcodes = user_df.groupBy("zipCode").count().count()

    println("num_users : " + user_df.count())
    println("num_genders : "+ num_genders)
    println("num_occupations : "+ num_occupations)
    println("num_zipcodes: " + num_zipcodes)
    println("Distribution by Occupation")
    println(user_df.groupBy("occupation").count().show())

  }
}
