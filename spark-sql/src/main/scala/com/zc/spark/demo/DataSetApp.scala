package com.zc.spark.demo

import org.apache.spark.sql.SparkSession

object DataSetApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DataSetApp")
      .master("local")
      .getOrCreate()
    val student = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/Users/zhongcheng/Documents/spark/student.csv")


    import spark.implicits._
    val ds = student.as[Student]

    ds.show()


  }

  case class Student(id: Int, name: String, mobile: String, email: String)

}
