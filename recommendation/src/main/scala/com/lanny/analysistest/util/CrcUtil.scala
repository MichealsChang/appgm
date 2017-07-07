package com.lanny.analysistest.util

object CrcUtil {
  def crcFormat(str: String) = {
    val sb = new StringBuilder(str);
    //头部补0
    while (sb.length < 8) {
      sb.insert(0, "0")
    }
    sb.toString()
  }
}