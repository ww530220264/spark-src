package org.apache.spark.examples.ww

import org.apache.spark.sql.SparkSession

object TestShuffle {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("").getOrCreate()
    val tuple2RDD = spark.sparkContext.parallelize(Seq(1,2),2).map((_,1))
    val groupByKeyRDD = tuple2RDD.groupByKey(2)
    val count = groupByKeyRDD.collect()
    count.foreach(x=>println((x._1,x._2.mkString("-"))))
  }
}
