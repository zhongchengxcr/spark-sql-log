package com.zc.spark

import com.ggstar.util.ip.IpHelper

object AccessConvert {


  def log2Type(log: String) = {


    try {
      val splits = log.split(" ")
      val ip = splits(0)
      val city = getCity(ip)
      val dateStr = DateUtils.parse(splits(3) + " " + splits(4))
      val day = dateStr.substring(0,10).replaceAll("-", "")
      val topcic = splits(6)
      val browser = splits(11)
      AccessLogInfo(day, ip, city, dateStr, topcic, browser)
    } catch {
      case e: Exception => null
    }

  }


  def getCity(ip: String) = {
    IpHelper.findRegionByIp(ip)
  }

  case class AccessLogInfo(day: String, ip: String, city: String, date: String, topic: String, browser: String)

}
