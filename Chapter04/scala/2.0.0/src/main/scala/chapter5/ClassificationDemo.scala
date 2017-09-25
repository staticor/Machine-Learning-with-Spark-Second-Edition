package chapter5

import org.apache.spark.{SparkConf, SparkContext}

object ClassificationDemo {


  def main(args: Array[String]): Unit = {
    val spConfig = new SparkConf().setMaster("local")
                   .setAppName("DemoStaticor")
    val sc = new SparkContext(config=spConfig)
    val raw = sc.textFile("/Users/steveyoung/train_noheader.tsv")

    println(raw.first())
  }
}
