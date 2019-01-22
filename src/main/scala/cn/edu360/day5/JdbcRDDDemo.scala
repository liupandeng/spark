package cn.edu360.day5

import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zx on 2017/10/10.
  */
object JdbcRddDemo {

  val getConn = () => {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/test?characterEncoding=UTF-8", "root", "root")
  }


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("JdbcRddDemo").setMaster("local[*]")

    val sc = new SparkContext(conf)

    //创建RDD，这个RDD会记录以后从MySQL中读数据

    //new 了RDD，里面没有真正要计算的数据，而是告诉这个RDD，以后触发Action时到哪里读取数据
    val jdbcRDD: RDD[(String, Int)] = new JdbcRDD(
      sc,
      getConn,
      "SELECT * FROM access_log WHERE num >= ? AND num < ?",
      200,
      900,
      2, //分区数量
      rs => {
        val province = rs.getString(1)
        val counts = rs.getInt(2)
        (province, counts)
      }
    )

    //


    //触发Action
    val r = jdbcRDD.collect()

    println(r.toBuffer)

    sc.stop()


  }

}