package com.zc.spark

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {

  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  //目标日期格式
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")


  def getTime(time: String): Long = {
    val dataStr = time.substring(time.indexOf("[") + 1, time.indexOf("]"))
    YYYYMMDDHHMM_TIME_FORMAT.parse(dataStr).getTime()
  }


  def parse(time: String): String = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  def main(args: Array[String]) {
    println(parse("[18/Sep/2013:06:49:33 +0000]"))
  }
}
