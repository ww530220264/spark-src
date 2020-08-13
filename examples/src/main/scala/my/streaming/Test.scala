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

package org.apache.spark.streaming.scheduler.rate

import org.apache.spark.internal.Logging

/**
 * Implements a proportional-integral-derivative (PID) controller which acts on
 * the speed of ingestion of elements into Spark Streaming. A PID controller works
 * by calculating an '''error''' between a measured output and a desired value. In the
 * case of Spark Streaming the error is the difference between the measured processing
 * rate (number of elements/processing delay) and the previous rate.
 * 在Spark Streaming中Error是当前处理速率和之前的处理速率之差
 * @see <a href="https://en.wikipedia.org/wiki/PID_controller">PID controller (Wikipedia)</a>
 *
 * @param batchIntervalMillis the batch duration, in milliseconds||
 * @param proportional how much the correction should depend on the current
 *        error. This term usually provides the bulk of correction and should be positive or zero.
 *        A value too large would make the controller overshoot the setpoint, while a small value
 *        would make the controller too insensitive. The default value is 1.
 * @param integral how much the correction should depend on the accumulation
 *        of past errors. This value should be positive or 0. This term accelerates the movement
 *        towards the desired value, but a large value may lead to overshooting. The default value
 *        is 0.2.
 * @param derivative how much the correction should depend on a prediction
 *        of future errors, based on current rate of change. This value should be positive or 0.
 *        This term is not used very often, as it impacts stability of the system. The default
 *        value is 0.
 * @param minRate what is the minimum rate that can be estimated.
 *        This must be greater than zero, so that the system always receives some data for rate
 *        estimation to work.
 */
//val proportional = conf.getDouble("spark.streaming.backpressure.pid.proportional", 1.0)
//val integral = conf.getDouble("spark.streaming.backpressure.pid.integral", 0.2)
//val derived = conf.getDouble("spark.streaming.backpressure.pid.derived", 0.0)
//val minRate = conf.getDouble("spark.streaming.backpressure.pid.minRate", 100)
//new PIDRateEstimator(batchInterval.milliseconds, proportional, integral, derived, minRate)
//在Spark Streaming中Error是当前处理速率和之前的处理速率之差
private[streaming] class PIDRateEstimator(
                                           batchIntervalMillis: Long,//一个batch的时长
                                           proportional: Double,//比例,默认1
                                           integral: Double,//积分比例：默认0.2
                                           derivative: Double,//微分比例：默认0.0
                                           minRate: Double//最小比例：默认100
                                         ) extends RateEstimator with Logging {

  private var firstRun: Boolean = true//是否是第一次运行
  private var latestTime: Long = -1L//最近一次的时间
  private var latestRate: Double = -1D//最近一次的摄取数据速率
  private var latestError: Double = -1L//最近一次误差

  def compute(
               time: Long, // in milliseconds 刚处理完成的batch interval的timestamp
               numElements: Long,//记录数量 在这个batch中处理完成的元素数量
               processingDelay: Long, // in milliseconds 处理时间延迟
               schedulingDelay: Long // in milliseconds 调度时间延迟
             ): Option[Double] = {
    this.synchronized {
      if (time > latestTime && numElements > 0 && processingDelay > 0) {
        // in seconds, should be close to batchDuration
        // 上次更新之后的到本次计算的实际间隔
        val delaySinceUpdate = (time - latestTime).toDouble / 1000
        // in elements/second
        // 本次每秒处理的元素个数
        val processingRate = numElements.toDouble / processingDelay * 1000
        // In our system `error` is the difference between the desired rate and the measured rate
        // based on the latest batch information. We consider the desired rate to be latest rate,
        // which is what this estimator calculated for the previous batch.
        // in elements/second
        // 误差是上次处理速率-本次处理速率
        val error = latestRate - processingRate

        // The error integral, based on schedulingDelay as an indicator for accumulated errors.
        // A scheduling delay s corresponds to s * processingRate overflowing elements. Those
        // are elements that couldn't be processed in previous batches, leading to this delay.
        // In the following, we assume the processingRate didn't change too much.
        // From the number of overflowing elements we can calculate the rate at which they would be
        // processed by dividing it by the batch interval. This rate is our "historical" error,
        // or integral part, since if we subtracted this rate from the previous "calculated rate",
        // there wouldn't have been any overflowing elements, and the scheduling delay would have
        // been zero.
        // (in elements/second)
        // schedulingDelay.toDouble * processingRate==延迟时间(毫秒)*1000(变成秒)*现在每秒钟处理速率==可以理解为调度延迟中的元素个数
        // 可以理解为调度延迟中的元素个数 / batchIntervalMills*1000(变成秒) == 理解为一个batch单位延迟了多少个元素,也就是误差速率
        val historicalError = schedulingDelay.toDouble * processingRate / batchIntervalMillis

        // in elements/(second ^ 2)
        // 微分
        val dError = (error - latestError) / delaySinceUpdate

        val newRate = (latestRate - proportional * error -
          integral * historicalError -
          derivative * dError).max(minRate)
        logTrace(s"""
                    | latestRate = $latestRate, error = $error
                    | latestError = $latestError, historicalError = $historicalError
                    | delaySinceUpdate = $delaySinceUpdate, dError = $dError
            """.stripMargin)

        latestTime = time
        if (firstRun) {
          latestRate = processingRate
          latestError = 0D
          firstRun = false
          logTrace("First run, rate estimation skipped")
          None
        } else {
          latestRate = newRate
          latestError = error
          logTrace(s"New rate = $newRate")
          Some(newRate)
        }
      } else {
        logTrace("Rate estimation skipped")
        None
      }
    }
  }
}
