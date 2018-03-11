package com.zc.spark.demo

import org.apache.spark.sql.SparkSession


object DFApiApp {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DFApiApp")
      .master("local")
      .getOrCreate()

    val student = spark.sparkContext.textFile("/Users/zhongcheng/Documents/spark/student.data")
    import spark.implicits._
    val studentDF = student.map(_.split("\\|")).map(line => Student(line(0).trim.toInt, line(1), line(2), line(3))).toDF()


    //false 代表超长不截取
    studentDF.show(25, truncate = false)

    studentDF.take(10)
    studentDF.first()
    studentDF.head(3)
    studentDF.select("name", "email")

    studentDF.filter("name='' or name = 'NULL'").show()

    studentDF.sort(studentDF.col("name").as("s_name").desc, studentDF.col("id").desc).show()


    val student1 = spark.sparkContext.textFile("/Users/zhongcheng/Documents/spark/student.data")
    val studentDF1 = student1.map(_.split("\\|")).map(line => Student(line(0).trim.toInt, line(1), line(2), line(3))).toDF()

    studentDF.join(studentDF1, studentDF.col("id") === studentDF1.col("id")).show()

    spark.stop()

  }

  case class Student(id: Int, name: String, mobile: String, email: String)

}
