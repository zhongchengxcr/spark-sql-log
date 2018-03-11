package com.zc.spark

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object TopNStatJob {


  //统计每个城市最受欢迎的浏览器的前三名
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("TopNStatJob")
      .master("local[2]")
      .getOrCreate()

    val accessDF = spark.read.json("/Users/zhongcheng/Documents/spark/output/access.json")

    val cityAccessTopNDF = accessDF
      .groupBy( accessDF.col("city"), accessDF.col("browser"))
      .agg(count("browser").as("times"))


    //统计每个城市最受欢迎的浏览器前3名
    val topNDF = cityAccessTopNDF.select(
      //cityAccessTopNDF.col("day"),
      cityAccessTopNDF.col("city"),
      cityAccessTopNDF.col("browser"),
      cityAccessTopNDF.col("times"),
      row_number().over(
        Window.partitionBy(
          cityAccessTopNDF.col("city")
        ).orderBy(cityAccessTopNDF.col("times").desc)
      ).as("times_rank")
    ).filter("times_rank <=3 ")

    topNDF.show(false)
    cityAccessTopNDF.show()
  }

}
