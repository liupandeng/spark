package cn.edu360.day6

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by zx on 2017/10/13.
  */
object SQLWordCount {

  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark = SparkSession.builder()
      .appName("SQLWordCount")
      .master("local[*]")
      .getOrCreate()

    //(指定以后从哪里)读数据，是lazy

    //Dataset分布式数据集，是对RDD的进一步封装，是更加智能的RDD
    //dataset只有一列，默认这列叫value
    val lines: Dataset[String] = spark.read.textFile("hdfs://node2:8020/aaa/ccc")

    //整理数据(切分压平)
    //导入隐式转换
    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))
   // words.show()

    //注册视图
    words.createTempView("v_wc")

    //方式1.执行SQL（Transformation，lazy）
//    val result: DataFrame = spark.sql("SELECT value word, COUNT(*) counts FROM v_wc GROUP BY word ORDER BY counts DESC")
//
//    //执行Action
//    result.show()


    //方式2.使用DataSet的API方式
//    val count  = words.groupBy($"value" as "word").count().sort($"count" desc)
//    count.show()

    //方式3.导入聚合函数
    import org.apache.spark.sql.functions._
    val counts  = words.groupBy($"value" as "word").agg(count("*") as "counts").orderBy($"counts" desc)
    counts.show()


    spark.stop()

  }
}
