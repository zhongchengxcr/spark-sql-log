package com.zc.spark

import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * 日志数据清洗
  */
object SparkStatCleanJob {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("SparkStatCleanJob")
      .master("local[2]")
      .getOrCreate()

    val accessLogRDD = spark.sparkContext.textFile("/Users/zhongcheng/Documents/spark/access.bak")

    import spark.implicits._
    val accessDF = accessLogRDD
      .map(line => AccessConvert.log2Type(line))
      .filter(_ != null).toDF()

    accessDF.coalesce(1).write
      .mode(SaveMode.Overwrite)
      .partitionBy("day")
      .json("/Users/zhongcheng/Documents/spark/output/access.json")


  }
}
