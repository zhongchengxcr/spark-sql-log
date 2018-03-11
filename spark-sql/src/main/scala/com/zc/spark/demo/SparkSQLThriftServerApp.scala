package com.zc.spark.demo

import java.sql.DriverManager

object SparkSQLThriftServerApp {

  def main(arg: Array[String]): Unit = {


    Class.forName("org.apache.hive.jdbc.HiveDriver")


    val conn = DriverManager.getConnection("", "", "")

    val pstmt = conn.prepareStatement("select * from emp")

    val rs = pstmt.executeQuery()

    while (rs.next()) {
      println(rs.getInt("id"))
      println(rs.getString("name"))
    }



    rs.close()
    pstmt.close()
    conn.close()
  }

}
