package com.zc.spark.demo

import org.apache.spark.sql.SparkSession

/**
  * Hello world!
  *
  */
object SparkSessionDemo {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

    //import spark.implicits._
    val people = spark.read.json("/Users/zhongcheng/Documents/spark/people.json")
    people.show()
  }
}
