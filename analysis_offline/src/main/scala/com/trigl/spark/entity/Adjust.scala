package com.trigl.spark.entity

class Adjust(
    var cpzthreshold: Int,
    var cpzratiolowerbound: Int,
    var cpzratioupperbound: Int,
    var ssratio: Int,
    var lsratio: Int,
    var jswratio: Int) {
  var customid: Int = 0
  var mainswitch: Int = 0
  var indivswitch: Int = 0
}