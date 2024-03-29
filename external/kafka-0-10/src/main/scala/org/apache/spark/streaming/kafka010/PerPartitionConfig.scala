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

import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkConf
import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * Interface for user-supplied configurations that can't otherwise be set via Spark properties,
 * because they need tweaking on a per-partition basis,
 */
@Experimental
abstract class PerPartitionConfig extends Serializable {
  /**
   *  Maximum rate (number of records per second) at which data will be read
   *  from each Kafka partition.
   */
  def maxRatePerPartition(topicPartition: TopicPartition): Long
  def minRatePerPartition(topicPartition: TopicPartition): Long = 1
}

/**
 * Default per-partition configuration
 */
private class DefaultPerPartitionConfig(conf: SparkConf)
    extends PerPartitionConfig {
  val maxRate = conf.getLong("spark.streaming.kafka.maxRatePerPartition", 0)
  val minRate = conf.getLong("spark.streaming.kafka.minRatePerPartition", 1)
  System.out.println(s"""【wangwei】线程：${Thread.currentThread().getName}，
        设置kafka每个partition摄取数据速率：maxRate：${maxRate},minRate：${minRate}""".stripMargin)

  def maxRatePerPartition(topicPartition: TopicPartition): Long = maxRate
  override def minRatePerPartition(topicPartition: TopicPartition): Long = minRate
}
