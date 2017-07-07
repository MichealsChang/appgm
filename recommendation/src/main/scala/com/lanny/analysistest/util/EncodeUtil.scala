package com.lanny.analysistest.util

import java.math.BigInteger
import java.security.MessageDigest

/**
  * 加解密工具
  * created by Trigl at 2017-05-08 14:10
  */
object EncodeUtil {

  def getMD5(str: String): String = {
    // 生成一个MD5加密计算摘要
    val md = MessageDigest.getInstance("MD5")
    // 计算md5函数
    md.update(str.getBytes())
    new BigInteger(1, md.digest()).toString(16)
  }

  def main(args: Array[String]) {
    println(getMD5("869271020906407"))
  }
}
