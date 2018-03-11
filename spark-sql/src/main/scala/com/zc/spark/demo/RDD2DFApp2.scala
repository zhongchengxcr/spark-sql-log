package com.zc.spark.demo

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object RDD2DFApp2 {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("RDD2DFApp2")
      .master("local")
      .getOrCreate()


    val peopleRDD = spark.sparkContext.textFile("/Users/zhongcheng/Documents/spark/people.txt")

    val schemaString = "name age"

    //    val fields = schemaString.split(" ")
    //      .map(filedName => StructField(filedName, StringType, nullable = true))
    //
    //    val fields1 = List[StructField]()


    val nameField = StructField("name", StringType, nullable = true)
    val ageField = StructField("age", IntegerType, nullable = true)


    val schema = StructType(Array(nameField, ageField))

    val rowRDD = peopleRDD.map(_.split(",")).map(line => Row(line(0), line(1).trim().toInt))


    val peopleDF = spark.createDataFrame(rowRDD, schema)


    peopleDF.createOrReplaceTempView("people")
    spark.sql("select age,count(age) from people group by age").show()
    peopleDF.show()

  }

}
