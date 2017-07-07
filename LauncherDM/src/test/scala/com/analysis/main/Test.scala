package com.analysis.main

import java.text.SimpleDateFormat
import java.util.{Calendar, Random}

import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.ListBuffer

/**
  *
  * @author 白鑫
  */
object Test {

  trait HasLegs {
    val legCount: Int = 4
    def walk {
      println("Use legs to walk!")
    }
    def run
  }

  class Dog extends HasLegs {
    override def run {
      println("I run very fast!")
    }
  }

  case class AppDetail(signcrc32: String, pkg: String, appname: String)

  def addMonth(oldDate: String): String ={
    val df = new SimpleDateFormat("yyyyMM")
    var date = df.parse(oldDate)
    val ca = Calendar.getInstance()
    ca.setTime(date)
    ca.add(Calendar.MONTH, 1) // 加一月
    date = ca.getTime()
    df.format(date)
  }

  // 生成insert语句
  def generateInsertSql(date1: String, date2: String): String = {

    val sb = new StringBuilder
    val ca = Calendar.getInstance()

    val df = new SimpleDateFormat("yyyyMMdd")
    val yearDF = new SimpleDateFormat("yyyy")
    val monthDF = new SimpleDateFormat("MM")
    val dayDF = new SimpleDateFormat("dd")


    // 开始时间必须小于结束时间
    val beginDate = df.parse(date1)
    val endDate = df.parse(date2)
    var date = beginDate
    while (!date.equals(endDate)) {
      sb.append("INSERT OVERWRITE TABLE transform_appinfo\n")
      sb.append("PARTITION (year=" + yearDF.format(date) + ",month=" + monthDF.format(date) + ",day=" + dayDF.format(date) + ")\n")
      sb.append("SELECT a.pkg,a.appname,a.signcrc32,a.opttime,a.firstimei,a.secondimei,a.thirdimei WHERE a.year=" + yearDF.format(date) + " and a.month=" + monthDF.format(date) + " and a.day=" + dayDF.format(date) + "\n")
      ca.setTime(date)
      ca.add(Calendar.DATE, 1) // 日期加1天
      date = ca.getTime()
    }
    sb.toString()

  }

  def checkCrc(crc: String): String = {
    var crc32 = crc
    if (crc.length() < 8)
    // 补零
    /*      for (i <- 0 to crc.length-1)
            crc32 += "0"*/
      crc32 = String.format("%1$0" + (8 - crc.length()) + "d", new Integer(0)) + crc
    crc32
  }

  def main(args: Array[String]) {
    println(StringUtils.isNotBlank(null))
//    print(new Random().nextInt(10))

//    print(checkCrc("afasd"))

/*    val str = "201611"
    val insertSql = "FROM app a\n" + generateInsertSql(str + "01", addMonth(str) + "01")
    println(insertSql)*/

/*    val str = "024|023|028|049|017|008|010|007|129|029|002|018|014"
    val ids = str.split("\\|")
    for (i <- 0 until ids.length) {
      for (j <- i+1 until ids.length)
        if (ids(i).toInt < ids(j).toInt) {
          println(ids(i) + ids(j))
        } else {
          println(ids(j) + ids(i))
        }
    }*/


  }
}
