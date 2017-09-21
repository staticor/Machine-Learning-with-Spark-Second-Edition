

import java.awt.Font

import org.apache.spark.sql.DataFrame
import org.jfree.chart.axis.{CategoryLabelPosition, CategoryLabelPositions}
import org.sparksamples.Util._
import org.sparksamples.{MovieData, Util}

import scalax.chart.module.ChartFactories

object DatasetExploreDemo {

  def convert(x: String) : Integer = 1998 - x.toInt
  def convert(x: Integer) : Integer = 1998 - x
  def convertYear( x:String) : Int = {
    try
      x.takeRight(4).toInt
    catch {
      case e: Exception => println("exception caught: " + e + " Returning 1900");
        1900
    }
  }
  def getMovieYearsCountSorted() : scala.Array[(Int, String)] = {
    val movie_data_df = Util.getMovieDataDF()
    movie_data_df.createOrReplaceTempView("movie_data")
    movie_data_df.printSchema()

    Util.spark.udf.register("convertYear", convertYear _)
    movie_data_df.show(false)

    val movie_years = Util.spark.sql(
      "select convertYear(date) as year from movie_data"
    )
    val movie_years_count: DataFrame = movie_years.groupBy("year").count()
    movie_years_count.show(false)

    val movie_years_count_rdd = movie_years_count.rdd.map(
      row => (Integer.parseInt(row(0).toString), row(1).toString))

    val movie_years_count_collect = movie_years_count_rdd.collect()
    val movie_years_count_collect_sort = movie_years_count_collect.sortBy(_._1)
    movie_years_count_collect_sort
  }



  def main(args: Array[String]): Unit = {
     val user_df = getUserFieldDataFrame
     val occupation = user_df.select("occupation")
     val occupation_groups = user_df.groupBy("occupation").count()
     val occupation_groups_sorted = occupation_groups.sort("count")
//     println(user_df)
     occupation_groups_sorted.show()

     val occupation_groups_collection = occupation_groups_sorted.collect()

     val ds = new org.jfree.data.category.DefaultCategoryDataset
     val mx = scala.collection.immutable.ListMap()

     for(x <- 0 until occupation_groups_collection.length){
       val occ = occupation_groups_collection(x)(0)
       val count = Integer.parseInt(
         occupation_groups_collection(x)(1).toString
       )
       ds.addValue(count, "UserAges", occ.toString)
     }

     val chart = ChartFactories.BarChart(ds)
     val font = new Font("Dialog", Font.PLAIN, 5)

     chart.peer.getCategoryPlot.getDomainAxis()
       .setCategoryLabelPositions(CategoryLabelPositions.UP_90)
     chart.peer.getCategoryPlot.getDomainAxis
       .setLabelFont(font)

     chart.show()

    println("=================")

     val movie_data = getMovieData()

     movie_data.take(3).foreach(println)

    println("=================")
     val movie_years = getMovieYearsCountSorted()
     for(a<- 0 to (movie_years.length - 1)) {
       print(movie_years(a))
     }

     Util.sc.stop()
   }


}
