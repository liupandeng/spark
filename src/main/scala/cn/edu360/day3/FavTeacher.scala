package cn.edu360.day3

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zx on 2017/10/8.
  * 最受欢迎老师
  */
object FavTeacher {

  def main(args: Array[String]): Unit = {
//   setMaster
// 　The master URL to connect to, such as "local" to run locally with one thread,
// 　"local[4]" to run locally with 4 cores,
// 　or "spark://master:7077" to run on a Spark standalone cluster.
//   Spark属性控制大部分的应用程序设置， Spark属性可以直接在SparkConf上配置，然后传递给SparkContext
    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //指定以后从哪里读取数据
    val lines: RDD[String] = sc.textFile("data/teacher.log")
    //整理数据
    val teacherAndOne = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      //val httpHost = line.substring(0, index)
      //val subject = new URL(httpHost).getHost.split("[.]")(0)
      (teacher, 1)
    })
    //聚合
    val reduced: RDD[(String, Int)] = teacherAndOne.reduceByKey(_+_)
    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //触发Action执行计算
    val reslut: Array[(String, Int)] = sorted.collect()

    //打印
    println(reslut.toBuffer)

    sc.stop()




  }
}
