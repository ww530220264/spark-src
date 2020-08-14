package org.apache.spark.examples.ww

import org.apache.spark.sql.SparkSession

object TestShuffle {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val tuple2RDD = spark.sparkContext.parallelize(Seq(1, 2), 2).map((_, 1))
    val groupByKeyRDD = tuple2RDD.groupByKey(2)
    val count = groupByKeyRDD.collect()
    count.foreach(x => println((x._1, x._2.mkString("-"))))
//    val peopleRDD = spark.sparkContext.parallelize(List(("Mobin", 2), ("Mobin", 1), ("Lucy", 2), ("Lucy", 1), ("Amy", 3), ("Amy", 1)), 4)
//    peopleRDD.foldByKey(2)(_ + _).foreach(println)
//    peopleRDD.repartition(5).foldByKey(2)(_ + _).foreach(println)
    //    peopleRDD.combineByKey(
    //      x => x + 2,
    //      (a: Int, b: Int) => a + b,
    //      (a: Int, b: Int) => a + b
    //    ).foreach(println)

    //    peopleRDD.reduceByKey(_+_).foreach(println)

    //    peopleRDD.combineByKey(
    //      x => (List(x), 1),
    //      (tuple: (List[Int], Int), x: Int) => (x :: tuple._1, tuple._2 + 1),
    //      (a: (List[Int], Int), b: (List[Int], Int)) => (a._1 ::: b._1, a._2 + b._2)
    //    ).foreach(println)
  }
}
