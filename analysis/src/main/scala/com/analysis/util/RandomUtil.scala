package com.analysis.util

import java.util.Random

object RandomUtil {

  val ALLCHAR = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  val LETTERCHAR = "abcdefghijkllmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  val NUMBERCHAR = "0123456789";

  /**
   * 返回一个定长的随机字符串(只包含大小写字母、数字)
   *
   * @param length
   *            随机字符串长度
   * @return 随机字符串
   */
  def generateString(length: Int): String = {
    val sb = new StringBuffer()
    val random = new Random(System.nanoTime())
    for (i <- 0 until length) {
      sb.append(ALLCHAR.charAt(random.nextInt(ALLCHAR.length())))
    }
    sb.toString()
  }

  /**
   * 返回一个定长的随机纯字母字符串(只包含大小写字母)
   *
   * @param length
   *            随机字符串长度
   * @return 随机字符串
   */
  def generateMixString(length: Int): String = {
    val sb = new StringBuffer()
    val random = new Random(System.nanoTime())
    for (i <- 0 until length) {
      sb.append(ALLCHAR.charAt(random.nextInt(LETTERCHAR.length())))
    }
    sb.toString()
  }

  /**
   * 返回一个定长的随机纯大写字母字符串(只包含大小写字母)
   *
   * @param length
   *            随机字符串长度
   * @return 随机字符串
   */
  def generateLowerString(length: Int): String = {
    generateMixString(length).toLowerCase()
  }

  /**
   * 返回一个定长的随机纯小写字母字符串(只包含大小写字母)
   *
   * @param length
   *            随机字符串长度
   * @return 随机字符串
   */
  def generateUpperString(length: Int): String = {
    generateMixString(length).toUpperCase()
  }

  /**
   * 生成一个定长的纯0字符串
   *
   * @param length
   *            字符串长度
   * @return 纯0字符串
   */
  def generateZeroString(length: Int): String = {
    val sb = new StringBuffer()
    for (i <- 0 until length) {
      sb.append('0')
    }
    sb.toString()
  }

  /**
   * 根据数字生成一个定长的字符串，长度不够前面补0
   *
   * @param num
   *            数字
   * @param fixdlenth
   *            字符串长度
   * @return 定长的字符串
   */
  def toFixdLengthString(num: Long, fixdlenth: Int): String = {
    val sb = new StringBuffer()
    val strNum = String.valueOf(num)
    if (fixdlenth - strNum.length() >= 0) {
      sb.append(generateZeroString(fixdlenth - strNum.length()))
    } else {
      throw new RuntimeException("将数字" + num + "转化为长度为" + fixdlenth
        + "的字符串发生异常！")
    }
    sb.append(strNum)
    sb.toString()
  }

  /**
   * 每次生成的len位数都不相同
   *
   * @param param
   * @return 定长的数字
   */
  def getNotSimple(param: Array[Int], len: Int): Int = {
    val rand = new Random(System.nanoTime())
    for (i <- param.length until (1, -1)) {
      val index = rand.nextInt(i)
      val tmp = param(index)
      param(index) = param(i - 1)
      param(i - 1) = tmp
    }
    var result = 0
    for (i <- 0 until len) {
      result = result * 10 + param(i)
    }
    result
  }

  def main(args: Array[String]) {
    System.out.println("返回一个定长的随机字符串(只包含大小写字母、数字):" + generateString(10));
    System.out
      .println("返回一个定长的随机纯字母字符串(只包含大小写字母):" + generateMixString(10));
    System.out.println("返回一个定长的随机纯大写字母字符串(只包含大小写字母):"
      + generateLowerString(10));
    System.out.println("返回一个定长的随机纯小写字母字符串(只包含大小写字母):"
      + generateUpperString(10));
    System.out.println("生成一个定长的纯0字符串:" + generateZeroString(10));
    System.out.println("根据数字生成一个定长的字符串，长度不够前面补0:"
      + toFixdLengthString(123, 10));
    val in = Array(1, 2, 3, 4, 5, 6, 7);
    System.out.println("每次生成的len位数都不相同:" + getNotSimple(in, 3));
  }
}