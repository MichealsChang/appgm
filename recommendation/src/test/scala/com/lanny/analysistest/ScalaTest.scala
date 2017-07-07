package com.lanny.analysistest

import breeze.numerics.sqrt

import scala.collection.mutable.ListBuffer

/**
  *
  * @author 白鑫
  */
object ScalaTest {

  def main(args: Array[String]) {
    val d1 = 11451
    val d2 = 480865
    val d3 = 15787
    val b = BigDecimal(d2*d3)
    val b2 = BigInt(d2)
    val b3 = BigInt(d3)
    val d4 = d1 / sqrt(b.toDouble)
    println(d4)
  }

  def doubleFunc(iter: Iterator[Int]) : Iterator[(Int,Int)] = {
    var res = List[(Int,Int)]()
    while (iter.hasNext)
    {
      val cur = iter.next
      res .::= (cur,cur*2)
    }
    res.iterator
  }
}
