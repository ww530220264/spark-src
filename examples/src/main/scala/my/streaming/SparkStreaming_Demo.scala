package my.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming_Demo {
  def main(args: Array[String]): Unit = {
    val checkPointDir = "E:\\sourcecode\\spark\\spark-2.4.4\\check_point_dir\\SparkStreaming_Demo"
    // 当应用失败后重启，会检查是否存在checkPointDir，如果存在的话会恢复相应的数据
    val ssc = StreamingContext.getOrCreate(checkPointDir,()=>{
      val conf = new SparkConf()
      conf.setAppName("SparkStreaming_Demo")
      conf.setMaster("local[5]")
      val ssc = new StreamingContext(conf,Seconds(2))
      ssc.checkpoint(checkPointDir)
      ssc.sparkContext.setLogLevel("ERROR")

      val socketStreaming = ssc.socketTextStream("centos7-1", 9012)
      val words = socketStreaming.flatMap(_.split(" ")).map((_, 1))
      val states = words.updateStateByKey((vals, total: Option[Long]) => {
        total match {
          case Some(total) => Some(vals.sum + total)
          case None => Some(vals.sum.toLong)
        }
      })
      states.foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          partition.foreach(println)
        })
      })
      ssc
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
