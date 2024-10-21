package com.yz.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {

  def main(args: Array[String]): Unit = {

    // Application
    // Spark框架

    // 建立和Spark框架的连接
    // JDBC: Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    // 执行业务操作

    // 1、读取文件 一行一行读取
    // hello world
    val lines:RDD[String] = sc.textFile("datas")

    // 2、将一行数据进行拆分，形成一个一个的单词（分词）
    // 扁平化：将整体拆分为个体的操作
    val words:RDD[String] = lines.flatMap(_.split(" "))


    val wordToOne = words.map{
      word => (word,1)
    }
    // Spark框架提供了更多的功能，可以将分组和聚合使用一个方法实现
    // reduceByKey:相同的key的数据，可以对Value进行reduce聚合
    val wordCount = wordToOne.reduceByKey((key, value) => {key + value})

    // 5、将转换结果采集到控制台打印
    val arrays:Array[(String,Int)] = wordCount.collect()
    arrays.foreach(println)

    // 关闭连接
    sc.stop()

  }
}
