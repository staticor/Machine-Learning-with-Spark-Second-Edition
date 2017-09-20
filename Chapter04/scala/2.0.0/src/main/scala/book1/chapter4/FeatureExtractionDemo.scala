package book1.chapter4

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}


object FeatureExtractionDemo {


  def main(args: Array[String]) : Unit = {
    val spConfig = (new SparkConf).setMaster("local").setAppName("SparkApp")
    val sc = new SparkContext(spConfig)
    sc.setLogLevel("WARN")

    val PATH = "/Users/steve/ml-100k/"
    val USER_PATH = PATH + "u.data"

    val rawData = sc.textFile(USER_PATH)

    println(rawData.first())
    // 196  242   3  881250949

    val rawRatings = rawData.map(line => line.split("\t").take(3))

    //rawRatings.take(3).foreach(println) // show top 3 raw record

    val ratings = rawRatings.map {case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble)}
    
     println(ratings.first())
    // Rating(196, 242, 3.0)
    
    val model: MatrixFactorizationModel = ALS.train(ratings, 50, 10, 0.01)
    
    val model_part1 = model.userFeatures
    println(model_part1.first())
    val model_part2 = model.productFeatures
    println(model_part2.first())
    
    //model predict
    //个体预测: 预测用户对指定物品的评价
    val predictedRating = model.predict(789, 123)
    println(predictedRating) // 2.304633
    
    // ALS 模型的初始是随机的, 所以 书中的结果为3.2 和我的运行结果2.3 有差异.
    
    
    // 个体预测2: 为某用户推荐 前K有价值的物品
    val userId = 789
    val K = 10
    
    val topKRecs = model.recommendProducts(userId, K)
    
    println(topKRecs.mkString("\n"))
    
    //检验推荐效果
    
    // 简单对比用户所评价过电影的标题和被推荐电影的名称
    val movies = sc.textFile(PATH + "u.item")
    val titles = movies.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt, array(1))).collectAsMap()
    
    println(titles(123))
    
    val moviesForUser = ratings.keyBy(_.user).lookup(789)
    println(moviesForUser.size)
    
    println("==================================")
    // 用户真实的评价
    moviesForUser.sortBy(_.rating).take(10).map(rating => (titles(rating.product), rating.rating)).foreach(println)
    println("===================================")
    // 模型推荐的预测结果
    topKRecs.map(rating => (titles(rating.product), rating.rating)).foreach(println)
  }
}
