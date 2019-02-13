package com.spark.test.utils



import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {
  val YYYYMMDDHHMMSS_Format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val Target_Format = FastDateFormat.getInstance("yyyyMMddHHmmss")

  def getTimetest(time:String) = {
    YYYYMMDDHHMMSS_Format.parse(time).getTime
  }

  def parseToMinute(time:String) = {
    Target_Format.format(new Date(getTimetest(time)))
  }

  def main(args: Array[String]): Unit = {
    println(parseToMinute("2017-10-22 14:46:05 "))
    println()
  }

}
