package com.trigl.spark

import org.apache.commons.lang3.time.FastDateFormat

/**
  *
  * created by Trigl at 2017-05-27 18:29
  */
object Test {
  val fdf = FastDateFormat.getInstance("yyyy/MM/dd/")

  def main(args: Array[String]) {
    val str = "863807033986419; "
    val imeis = str.split(";")
    println(imeis.length)
  }
}
