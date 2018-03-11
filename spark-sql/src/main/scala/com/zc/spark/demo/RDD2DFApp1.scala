package com.zc.spark.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RDD2DFApp1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val spark = SparkSession.builder()
      .config(conf)
      .appName("RDD2DFApp1")
      .master("local")
      .getOrCreate()


    val peopleRDD = spark.sparkContext.textFile("/Users/zhongcheng/Documents/spark/people.txt")

    import  spark.implicits._
    val peopleLineRDD = peopleRDD.map(_.split(",")).map(line => PeopleInfo(line(0), line(1).trim().toInt)).toDF()

    peopleLineRDD.show()

    peopleLineRDD.createOrReplaceTempView("people")


    spark.sql("select * from people where age >20").show()


    spark.stop()


  }


  case class PeopleInfo(name: String, age: Int)

}
