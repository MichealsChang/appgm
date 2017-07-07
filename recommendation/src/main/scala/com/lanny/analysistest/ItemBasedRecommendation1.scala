package com.lanny.analysistest

import breeze.numerics.sqrt
import com.lanny.analysistest.util.RandomUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * 基于物品的推荐系统
  * created by Trigl at 2017-06-20 10:54
  */
object ItemBasedRecommendation1 {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.sql.crossJoin.enabled", "true") // 允许join笛卡尔积
    System.setProperty("spark.kryoserializer.buffer.max", "128")
    val sparkConf = new SparkConf()
        .setAppName("ItemBasedRecommendation")
        .set("spark.rpc.netty.dispatcher.numThreads", "2") // 预防RpcTimeoutException
    val sc = new SparkContext(sparkConf)

    val data = sc.textFile("/test/ratings.dat", 10).map(f => {
      val arr = f.split("::")
      (arr(0), arr(1), arr(2).toDouble)
    })

    val sim = similarity(data, 100)
//    sim.take(1000).foreach(println)
    val result = recommend(sim, data, 10).map(f => f._1 + "::" + f._2 + "::" + f._3)
    result.repartition(1).saveAsTextFile("/test/movieLens")

    sc.stop()
  }

  /**
    * 物品相似度
    * @param user_rdd
    * @return
    */
  def similarity(user_rdd: RDD[(String, String, Double)], filterNum: Int): RDD[(String, String, Double)] = {

    // 0、数据做准备
    val user_rdd2 = user_rdd.map(f => (f._1, f._2)).sortByKey()
    user_rdd2.cache()

    // 1、（用户：物品） 笛卡尔积 （用户：物品） => 物品：物品组合
    val user_rdd3 = user_rdd2.join(user_rdd2)
    val user_rdd4 = user_rdd3.map(data => (data._2, 1))

    // 2、（物品：物品）：频次
    val user_rdd5 = user_rdd4.reduceByKey((x, y) => x + y)

    // 3、对角矩阵
    val user_rdd6 = user_rdd5.filter(f => f._1._1 == f._1._2)
    user_rdd6.cache()

    // 4、非对角矩阵
    val user_rdd7 = user_rdd5.filter(f => f._1._1 != f._1._2)

    // 5、计算同现相似度（物品1：物品2：同现频次）
    // 物品1：（（物品1：物品2：同现频次），物品1频次）
    val user_rdd8 = user_rdd7.map(f => (f._1._1, (f._1._1, f._1._2, f._2))).join(user_rdd6.map(f => (f._1._1, f._2)))
    val user_rdd9 = user_rdd8.map(f => (f._2._1._2, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._2)))
    // 物品2：（（物品1：物品2：同现频次：物品1频次），物品2频次）
    val user_rdd10 = user_rdd9.join(user_rdd6.map(f => (f._1._1, f._2)))
    // 物品1：物品2：同现频次：物品1频次：物品2频次
    val user_rdd11 = user_rdd10.map(f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2))
    // 物品1：(物品2：相似度)
    val user_rdd12 = user_rdd11.map(f => (f._1, (f._2, (f._3 / sqrt(f._4 * f._5)))))
    // 选出与物品1相似度排前100的物品2，以减少数据量
    val user_rdd13 = user_rdd12.map(f => {
      (f._1, f._2 :: Nil)
    }).reduceByKey(_ ::: _).map(f => {
      val i2 = f._2.toBuffer
      val i2_2 = i2.sortBy(_._2)(Ordering[Double].reverse) // 逆序
      if(i2_2.length > filterNum) i2_2.remove(filterNum, i2_2.length - filterNum)
      (f._1, i2_2.toIterator)
    })
    // 物品1：物品2：相似度
    val user_rdd14 = user_rdd13.flatMap(f => {
      val sims = f._2
      for(s <- sims) yield (f._1, s._1, s._2)
    })

    // 6、结果返回
    user_rdd14
  }

  /**
    * 给某用户推荐物品
    * @param items_similar
    * @param user_perf
    * @param r_number
    * @return
    */
  def recommend(items_similar: RDD[(String, String, Double)],
                user_perf: RDD[(String, String, Double)],
                r_number: Int): RDD[(String, String, Double)] = {

    val user = user_perf.map(f => (f._2 + "_" + RandomUtil.getRandom(100), (f._1, f._3))).partitionBy(new NumberPartitioner(100))
      .map(f => {
        val keyWithSuffix = f._1
        val key = keyWithSuffix.substring(0, keyWithSuffix.lastIndexOf("_"))
        (key, f._2)
      })
    val sim = items_similar.map(f => (f._1 + "_" + RandomUtil.getRandom(100), (f._1, f._3))).partitionBy(new NumberPartitioner(100))
      .map(f => {
        val keyWithSuffix = f._1
        val key = keyWithSuffix.substring(0, keyWithSuffix.lastIndexOf("_"))
        (key, f._2)
      })

    // 物品1：（（用户：用户对物品1评分）:（物品2：物品1、2相似度））
    val rdd_R2 = user.join(sim)

    // 计算用户对物品的预测分  （用户：物品2）：用户对物品1的评分*物品1、2的相似度
    val rdd_R3 = rdd_R2.map(f => {
      ((f._2._1._1, f._2._2._1), f._2._1._2 * f._2._2._2)
    })

    // 元素累加求和  用户：（物品：预测分）
    val rdd_R4 = rdd_R3.reduceByKey((x, y) => x + y).map(f => {
      (f._1._1, (f._1._2, f._2))
    })

    // 用户对结果排序，过滤
    val rdd_R5 = rdd_R4.groupByKey()
    val rdd_R6 = rdd_R5.map(f => {
      val i2 = f._2.toBuffer
      val i2_2 = i2.sortBy(_._2)(Ordering[Double].reverse) // 逆序
      if(i2_2.length > r_number) i2_2.remove(r_number, i2_2.length - r_number)
      (f._1, i2_2.toIterator)
    })

    val rdd_R7 = rdd_R6.flatMap(f => {
      val id2 = f._2
      for(w <- id2) yield (f._1, w._1, w._2)
    })

    rdd_R7
  }
}
