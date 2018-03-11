package com.zc.spark.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkDataFrameApp {


  def main(args: Array[String]): Unit = {


    val conf = new SparkConf();
    val spark = SparkSession.builder()
      .master("local")
      .appName("SparkDataFrameApp")
      .config(conf)
      .getOrCreate()

    val peopleDF = spark.read.json("/Users/zhongcheng/Documents/spark/people.json")

    peopleDF.printSchema()
    // peopleDF.show(10)

    //select name ,age + 10 as age2 from people
    peopleDF.select(peopleDF.col("name"), (peopleDF.col("age") + 10).as("age2")).show()


    //select * from people where age > 19
    peopleDF.filter(peopleDF.col("age") > 19).show()

    peopleDF.groupBy("age").count().show()

    spark.close()


  }

}
