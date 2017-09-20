package org.sparksamples.exploredataset

import java.io.File

import org.apache.spark.SparkContext
import org.jfree.chart.plot.PlotOrientation
import org.jfree.chart.{ChartFactory, ChartUtilities}
import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.data.statistics.{HistogramDataset, HistogramType}
import org.apache.log4j.{Logger, Level}
/**
  * Created by manpreet.singh on 27/02/16.
  */
object explore_users {

  def main(args: Array[String]) {
	  val logger = Logger.getLogger("org")
	  logger.setLevel(Level.ERROR)
    val sc = new SparkContext("local[2]", "Explore Users in Movie Dataset")
<<<<<<< HEAD
    sc.setLogLevel("ERROR")
    val mlpath = "/Users/steveyoung/ml-100k/"
    val user_fields = sc.textFile(mlpath + "u.user").map(line => line.split("\\|"))
      .map(records => (records(0), records(1), records(2), records(3), records(4)))
    // userid, age, gender, occupation, zipcode
=======

    // 1|24|M|technician|85711
    val user_fields = sc.textFile("/Users/Steve/ml-100k/u.user")
        .map(line => line.split("\\|"))
        .map(records => (records(0), records(1), records(2), records(3), records(4)))
>>>>>>> 40cc251d08389a87f1400e35a96a482d20b1ad86

    val num_users = user_fields.count()
    println("u.user  has totally " + num_users + " users")

    // print what rdd has ?
    user_fields.take(10).foreach(println)
    
    // use map to get new rdd  case data structure.
    println(user_fields.map(user_fields => user_fields._3).distinct().count())

    val num_genders = user_fields.map{case(id, age, gender, occupations, zip) => gender}.distinct().count()
    println(num_genders)

    val num_occupations = user_fields.map{case(id, age, gender, occupations, zip) => occupations}.distinct().count()
    println(num_occupations)

    val num_zipcodes = user_fields.map{case(id, age, gender, occupations, zip) => zip}.distinct().count()
    println(num_zipcodes)

    val dataset1 = new HistogramDataset()
    dataset1.setType(HistogramType.RELATIVE_FREQUENCY)
    val ages = user_fields.map(user_fields => user_fields._2.toDouble).collect()
    //ages.take(5).foreach(println)
    dataset1.addSeries("Histogram", ages, 20)
    val plotTitle1 = "Age Histogram";
    val xaxis1 = "age"
    val yaxis1 = "scale"
    val orientation1 = PlotOrientation.VERTICAL;
    val show1 = false
    val toolTips1 = false
    val urls1 = false
    val chart1 = ChartFactory.createHistogram( plotTitle1, xaxis1, yaxis1, dataset1, orientation1, show1, toolTips1, urls1);
<<<<<<< HEAD
    val width1 = 600
    val height1 = 400
    ChartUtilities.saveChartAsPNG(new File(mlpath + "plots/age_histogram.png"), chart1, width1, height1)
=======
    val width1 = 600;
    val height1 = 400;
    ChartUtilities.saveChartAsPNG(new File("/Users/steve/ml-100k/plots/age_histogram.png"), chart1, width1, height1);
>>>>>>> 40cc251d08389a87f1400e35a96a482d20b1ad86

    val occs = user_fields.map(user_fields => (user_fields._4,1)).reduceByKey(_+_).collect()
    val dataset2 = new DefaultCategoryDataset()
    occs.foreach(println)
    for (occ <- occs) {
      dataset2.setValue(occ._2, "count", occ._1)
    }

<<<<<<< HEAD
    val plotTitle2 = "Occ Histogram"
    val xaxis2 = "occ"
    val yaxis2 = "count"
    val orientation2 = PlotOrientation.VERTICAL
    val show2 = false
    val toolTips2 = false
    val urls2 = false
    val chart2 = ChartFactory.createBarChart( plotTitle2, xaxis2, yaxis2, dataset2, orientation2, show2, toolTips2, urls2)
    val width2 = 2000
    val height2 = 500
    ChartUtilities.saveChartAsPNG(new File(mlpath + "plots/occ_histogram.png"), chart2, width2, height2)
=======
    val plotTitle2 = "Occ Histogram";
    val xaxis2 = "occ";
    val yaxis2 = "count";
    val orientation2 = PlotOrientation.VERTICAL;
    val show2 = false;
    val toolTips2 = false;
    val urls2 = false;
    val chart2 = ChartFactory.createBarChart( plotTitle2, xaxis2, yaxis2, dataset2, orientation2, show2, toolTips2, urls2);
    val width2 = 2000;
    val height2 = 500;
    ChartUtilities.saveChartAsPNG(new File("/Users/steve/ml-100k/plots/occ_histogram.png"), chart2, width2, height2);
>>>>>>> 40cc251d08389a87f1400e35a96a482d20b1ad86

    sc.stop()
  }

}
