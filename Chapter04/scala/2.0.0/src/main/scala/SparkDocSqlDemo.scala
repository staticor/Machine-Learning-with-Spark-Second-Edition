import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object SparkDocSqlDemo {


  def main(args:Array[String]) : Unit = {
    val SPARK_HOME ="/Users/steveyoung/spark-2.2.0-bin-hadoop2.7/"
    val spConfig: SparkConf = (new SparkConf).setMaster("local").setAppName("SparkApp")
    val sc = new SparkContext(spConfig)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val spark: SparkSession = SparkSession
      .builder().master("local")
      .appName("Spark Basic Example 2.2.0")
      .getOrCreate()



    val df = spark.read.json("/Users/steveyoung/MachineLearningWithSpark/demodata/people.json")

    df.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+

    //
    df.printSchema() // age: long, name: string
//    root
//    |-- age: long (nullable = true)
//    |-- name: string (nullable = true)
    df.select("name").show()
    println("=======================")
   // df.select("name, age +1").show()  error

    println("=======================")
    df.filter("age > 21")


    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()

//    df.create

    case class Person(name: String, age: Long)
    // Encoders are created for case classes
//    val caseClassDS = Seq(Person("Andy", 32)).toDS()

  }
}
