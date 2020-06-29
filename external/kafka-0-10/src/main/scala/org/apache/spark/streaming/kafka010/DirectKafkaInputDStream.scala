/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.kafka010

import java.{util => ju}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.scheduler.rate.RateEstimator

/**
 * A DStream where
 * each given Kafka topic/partition corresponds to an RDD partition.
 * The spark configuration spark.streaming.kafka.maxRatePerPartition gives the maximum number
 * of messages
 * per second that each '''partition''' will accept.
 *
 * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
 *                         see [[LocationStrategy]] for more details.
 * @param consumerStrategy In most cases, pass in [[ConsumerStrategies.Subscribe]],
 *                         see [[ConsumerStrategy]] for more details
 * @param ppc              configuration of settings such as max rate on a per-partition basis.
 *                         see [[PerPartitionConfig]] for more details.
 * @tparam K type of Kafka message key
 * @tparam V type of Kafka message value
 */
private[spark] class DirectKafkaInputDStream[K, V](
                                                    _ssc: StreamingContext,
                                                    locationStrategy: LocationStrategy,
                                                    consumerStrategy: ConsumerStrategy[K, V],
                                                    ppc: PerPartitionConfig
                                                  ) extends InputDStream[ConsumerRecord[K, V]](_ssc) with Logging with CanCommitOffsets {

  private val initialRate = context.sparkContext.getConf.getLong("spark.streaming.backpressure.initialRate", 0)
  System.out.println(
    s"""---【wangwei】线程：${Thread.currentThread().getName}，
       |DirectKafkaInputDStream：初始背压initialRate--->${initialRate}
       |---""".stripMargin)

  val executorKafkaParams = {
    val ekp = new ju.HashMap[String, Object](consumerStrategy.executorKafkaParams)
    KafkaUtils.fixKafkaParams(ekp)
    ekp
  }
  System.out.println(
    s"""---【wangwei】线程：${Thread.currentThread().getName}，
       |InputStream：Executor Kafka参数--->${executorKafkaParams}
       |---""".stripMargin)

  protected var currentOffsets = Map[TopicPartition, Long]()

  @transient private var kc: Consumer[K, V] = null

  def consumer(): Consumer[K, V] = this.synchronized {
    if (null == kc) {
      kc = consumerStrategy.onStart(currentOffsets.mapValues(l => new java.lang.Long(l)).asJava)
    }
    kc
  }

  override def persist(newLevel: StorageLevel): DStream[ConsumerRecord[K, V]] = {
    logError("Kafka ConsumerRecord is not serializable. " +
      "Use .map to extract fields before calling .persist or .window")
    super.persist(newLevel)
  }

  protected def getBrokers = {
    val c = consumer
    val result = new ju.HashMap[TopicPartition, String]()
    val hosts = new ju.HashMap[TopicPartition, String]()
    val assignments = c.assignment().iterator()
    while (assignments.hasNext()) {
      val tp: TopicPartition = assignments.next()
      if (null == hosts.get(tp)) {
        val infos = c.partitionsFor(tp.topic).iterator()
        while (infos.hasNext()) {
          val i = infos.next()
          hosts.put(new TopicPartition(i.topic(), i.partition()), i.leader.host())
        }
      }
      result.put(tp, hosts.get(tp))
    }
    System.out.println(
      s"""【wangwei】线程：${Thread.currentThread().getName}，
         |获取brokers:${result}""".stripMargin)
    result
  }

  protected def getPreferredHosts: ju.Map[TopicPartition, String] = {
    locationStrategy match {
      case PreferBrokers => getBrokers
      case PreferConsistent => ju.Collections.emptyMap[TopicPartition, String]()
      case PreferFixed(hostMap) => hostMap
    }
  }

  // Keep this consistent with how other streams are named (e.g. "Flume polling stream [2]")
  private[streaming] override def name: String = s"Kafka 0.10 direct stream [$id]"

  protected[streaming] override val checkpointData =
    new DirectKafkaInputDStreamCheckpointData


  /**
   * Asynchronously maintains & sends new rate limits to the receiver through the receiver tracker.
   */
  override protected[streaming] val rateController: Option[RateController] = {
    System.out.println(s"""【wangwei】线程：${Thread.currentThread().getName}，kafkaStream创建RateController,Controller中使用PIDRateEstimator""")
    if (RateController.isBackPressureEnabled(ssc.conf)) {
      System.out.println(
        s"""【wangwei】线程：${Thread.currentThread().getName}，
           |inputStream启用了速率控制器:${this}
           |""".stripMargin)
      Some(new DirectKafkaRateController(id,
        RateEstimator.create(ssc.conf, context.graph.batchDuration)))
    } else {
      None
    }
  }

  protected[streaming] def maxMessagesPerPartition(offsets: Map[TopicPartition, Long]): Option[Map[TopicPartition, Long]] = {
    // 每个分区->每个分区最高的offset
    System.out.println(
      s"""【wangwei】线程：${Thread.currentThread().getName}，
         |获取每个分区最大的消息数量[一个batchInterval]""".stripMargin)
    // 初始化RateLimit
    // 获取最近计算的rateLimit
    val estimatedRateLimit = rateController.map { x => {
      val lr = x.getLatestRate()
      if (lr > 0) lr else initialRate
    }
    }
    System.out.println(
      s"""${Thread.currentThread().getName}
         |最近计算的rateLimit:${estimatedRateLimit}
         |""".stripMargin)
    // calculate a per-partition rate limit based on current lag
    // 根据当前partition-offset-lag计算每个分区的最大获取消息数量
    val effectiveRateLimitPerPartition = estimatedRateLimit.filter(_ > 0) match {
      case Some(rate) =>
        // 根据当前partition-offset-lag计算每个分区的rate-limit
        // 每个分区最大的offset--当前每个分区已经消费到的offset
        val lagPerPartition = offsets.map { case (tp, offset) =>
          tp -> Math.max(offset - currentOffsets(tp), 0)
        }
        // 总的offset-lag
        val totalLag = lagPerPartition.values.sum
        System.out.println(
          s"""${Thread.currentThread().getName}
             |每个分区lag:${lagPerPartition}
             |总的lag:${totalLag}
             |""".stripMargin)
        lagPerPartition.map { case (tp, lag) =>
          // 每个分区最大rate-limit:spark.streaming.kafka.maxRatePerPartition
          val maxRateLimitPerPartition = ppc.maxRatePerPartition(tp)
          // 每个分区的backpressureRate=每个分区的lag/总的lag * 计算的rate
          val backpressureRate = lag / totalLag.toDouble * rate
          // 取maxRateLimitPerPatition和backpressureRate的较小值
          tp -> (if (maxRateLimitPerPartition > 0) {
            Math.min(backpressureRate, maxRateLimitPerPartition)
          } else backpressureRate)
        }
      case None => offsets.map { case (tp, offset) => tp -> ppc.maxRatePerPartition(tp).toDouble }
    }
    System.out.println(
      s"""${Thread.currentThread().getName}
         |每个分区的速率:${effectiveRateLimitPerPartition}
         |""".stripMargin)
    if (effectiveRateLimitPerPartition.values.sum > 0) {
      // batchDuration秒数
      val secsPerBatch = context.graph.batchDuration.milliseconds.toDouble / 1000
      Some(effectiveRateLimitPerPartition.map {
        // 计算每个分区每个batch interval获取的记录数量和最小rate的较大值
        case (tp, limit) => tp -> Math.max((secsPerBatch * limit).toLong, ppc.minRatePerPartition(tp))
      })
    } else {
      None
    }
  }

  /**
   * The concern here is that poll might consume messages despite being paused,
   * which would throw off consumer position.  Fix position if this happens.
   */
  private def paranoidPoll(c: Consumer[K, V]): Unit = {
    // don't actually want to consume any messages, so pause all partitions
    // 由于不想消费信息,因此暂停相关分区
    c.pause(c.assignment()) //暂停
    val msgs = c.poll(0) //立刻返回
    if (!msgs.isEmpty) { //如果返回消息的话,将offset重置为最小的offset作为补偿
      // position should be minimum offset per topicpartition
      msgs.asScala.foldLeft(Map[TopicPartition, Long]()) { (acc, m) =>
        val tp = new TopicPartition(m.topic, m.partition)
        val off = acc.get(tp).map(o => Math.min(o, m.offset)).getOrElse(m.offset)
        acc + (tp -> off)
      }.foreach { case (tp, off) =>
        logInfo(s"poll(0) returned messages, seeking $tp to $off to compensate")
        c.seek(tp, off)
      }
    }
  }

  /**
   * Returns the latest (highest) available offsets, taking new partitions into account.
   */
  protected def latestOffsets(): Map[TopicPartition, Long] = {
    val c = consumer
    paranoidPoll(c)
    // 当前分区-->偏移量
    val parts = c.assignment().asScala
    // 新加入的partition
    // make sure new partitions are reflected in currentOffsets
    val newPartitions = parts.diff(currentOffsets.keySet)

    // Check if there's any partition been revoked because of consumer rebalance.
    val revokedPartitions = currentOffsets.keySet.diff(parts)
    // 当前currentOffset需要被包含在part中
    if (revokedPartitions.nonEmpty) {
      throw new IllegalStateException(s"Previously tracked partitions " +
        s"${revokedPartitions.mkString("[", ",", "]")} been revoked by Kafka because of consumer " +
        s"rebalance. This is mostly due to another stream with same group id joined, " +
        s"please check if there're different streaming application misconfigure to use same " +
        s"group id. Fundamentally different stream should use different group id")
    }

    // position for new partitions determined by auto.offset.reset if no commit
    currentOffsets = currentOffsets ++ newPartitions.map(tp => tp -> c.position(tp)).toMap

    // find latest available offsets
    c.seekToEnd(currentOffsets.keySet.asJava)
    // 获取每个分区最新的可用的offset
    System.err.println(
      s"""${Thread.currentThread().getName},
         |获取最新的可用的offset:${parts.map(tp => tp -> c.position(tp)).toMap}""".stripMargin)
    parts.map(tp => tp -> c.position(tp)).toMap
  }

  // limits the maximum number of messages per partition
  protected def clamp(offsets: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    // offsets:每个分区-->每个分区当前最高的offset
    maxMessagesPerPartition(offsets).map { mmp =>
      mmp.map { case (tp, messages) =>
        val uo = offsets(tp)
        System.out.println(
          s"""${Thread.currentThread().getName}
             |计算的每个分区待消费的消息数量:${tp.topic()}-${tp.partition()}-${messages}
             |当前每个分区最大的消息偏移量:${tp.topic()}-${tp.partition()}-${uo}
             |""".stripMargin)
        tp -> Math.min(currentOffsets(tp) + messages, uo)
      }
    }.getOrElse(offsets)
  }

  override def compute(validTime: Time): Option[KafkaRDD[K, V]] = {
    val untilOffsets = clamp(latestOffsets())
    // 偏移量范围
    val offsetRanges = untilOffsets.map { case (tp, uo) =>
      val fo = currentOffsets(tp) //当前topicPartition的offset  uo-限制的每个partition消息的数量+current topic partition offset
      System.out.println(
        s"""${Thread.currentThread().getName}
           |最终每个分区待消费的消息数量:${tp.topic()}-${tp.partition()}-${fo}--${uo}
           |""".stripMargin)
      OffsetRange(tp.topic, tp.partition, fo, uo)
    }
    val useConsumerCache = context.conf.getBoolean("spark.streaming.kafka.consumer.cache.enabled",
      true)
    val rdd = new KafkaRDD[K, V](context.sparkContext, executorKafkaParams, offsetRanges.toArray,
      getPreferredHosts, useConsumerCache)

    // Report the record number and metadata of this batch interval to InputInfoTracker.
    val description = offsetRanges.filter { offsetRange =>
      // Don't display empty ranges.
      offsetRange.fromOffset != offsetRange.untilOffset
    }.map { offsetRange =>
      s"topic: ${offsetRange.topic}\tpartition: ${offsetRange.partition}\t" +
        s"offsets: ${offsetRange.fromOffset} to ${offsetRange.untilOffset}"
    }.mkString("\n")
    // Copy offsetRanges to immutable.List to prevent from being modified by the user
    val metadata = Map(
      "offsets" -> offsetRanges.toList,
      StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
    val inputInfo = StreamInputInfo(id, rdd.count, metadata)
    System.out.println(s"""【wangwei】线程：${Thread.currentThread().getName}，将本次batch interval的记录数量等元数据汇报给InputInfoTracker""")
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

    currentOffsets = untilOffsets
    commitAll()
    Some(rdd)
  }

  override def start(): Unit = {
    val c = consumer
    paranoidPoll(c)
    if (currentOffsets.isEmpty) {
      currentOffsets = c.assignment().asScala.map { tp =>
        tp -> c.position(tp)
      }.toMap
    }
  }

  override def stop(): Unit = this.synchronized {
    if (kc != null) {
        kc.close()
    }
  }

  protected val commitQueue = new ConcurrentLinkedQueue[OffsetRange]
  protected val commitCallback = new AtomicReference[OffsetCommitCallback]

  /**
   * Queue up offset ranges for commit to Kafka at a future time.  Threadsafe.
   *
   * @param offsetRanges The maximum untilOffset for a given partition will be used at commit.
   */
  def commitAsync(offsetRanges: Array[OffsetRange]): Unit = {
    commitAsync(offsetRanges, null)
  }

  /**
   * Queue up offset ranges for commit to Kafka at a future time.  Threadsafe.
   *
   * @param offsetRanges The maximum untilOffset for a given partition will be used at commit.
   * @param callback     Only the most recently provided callback will be used at commit.
   */
  def commitAsync(offsetRanges: Array[OffsetRange], callback: OffsetCommitCallback): Unit = {
    commitCallback.set(callback)
    // 将offsetRanges提交到commitQueue
    commitQueue.addAll(ju.Arrays.asList(offsetRanges: _*))
  }

  protected def commitAll(): Unit = {
    // 从commitQueue中获取offsetRange
    val m = new ju.HashMap[TopicPartition, OffsetAndMetadata]()
    var osr = commitQueue.poll()
    while (null != osr) {
      val tp = osr.topicPartition
      val x = m.get(tp)
      val offset = if (null == x) {
        osr.untilOffset
      } else {
        Math.max(x.offset, osr.untilOffset)
      }
      m.put(tp, new OffsetAndMetadata(offset))
      osr = commitQueue.poll()
    }
    if (!m.isEmpty) {
      System.out.println(
        s"""【wangwei】线程：${Thread.currentThread().getName}，
           | 从commitQueue中获取到offsetRange,进行异步commitAsync提交:
           | ${m.keySet().toArray.map(x => (x, m.get(x))).mkString("\n")}
           |""".stripMargin)
      consumer.commitAsync(m, commitCallback.get)
    }
  }

  private[streaming]
  class DirectKafkaInputDStreamCheckpointData extends DStreamCheckpointData(this) {
    def batchForTime: mutable.HashMap[Time, Array[(String, Int, Long, Long)]] = {
      data.asInstanceOf[mutable.HashMap[Time, Array[OffsetRange.OffsetRangeTuple]]]
    }

    override def update(time: Time): Unit = {
      batchForTime.clear()
      generatedRDDs.foreach { kv =>
        val a = kv._2.asInstanceOf[KafkaRDD[K, V]].offsetRanges.map(_.toTuple).toArray
        batchForTime += kv._1 -> a
      }
    }

    override def cleanup(time: Time): Unit = {}

    override def restore(): Unit = {
      batchForTime.toSeq.sortBy(_._1)(Time.ordering).foreach { case (t, b) =>
        logInfo(s"Restoring KafkaRDD for time $t ${b.mkString("[", ", ", "]")}")
        generatedRDDs += t -> new KafkaRDD[K, V](
          context.sparkContext,
          executorKafkaParams,
          b.map(OffsetRange(_)),
          getPreferredHosts,
          // during restore, it's possible same partition will be consumed from multiple
          // threads, so do not use cache.
          false
        )
      }
    }
  }

  /**
   * A RateController to retrieve the rate from RateEstimator.
   */
  private[streaming] class DirectKafkaRateController(id: Int, estimator: RateEstimator)
    extends RateController(id, estimator) {
    override def publish(rate: Long): Unit = ()
  }

}
