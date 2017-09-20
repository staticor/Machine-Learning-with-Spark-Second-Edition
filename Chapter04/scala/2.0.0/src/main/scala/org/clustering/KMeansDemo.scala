package org.clustering

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object KMeansDemo {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Clustering Demo")
    val sc = new SparkContext(conf)
    val PATH = "/Users/steveyoung/MachineLearningWithSpark/demodata/"
    val data = sc.textFile(PATH + "kmeans_data.txt")
    var parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)
    )).cache()

    // cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    // Evaluate clustering by computing within set sum of squared errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    // Save and load model
    clusters.save(sc, "target/org/apache/spark/KMeansExample/KmeansModel")
    val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KmeansModel")

  }

}
