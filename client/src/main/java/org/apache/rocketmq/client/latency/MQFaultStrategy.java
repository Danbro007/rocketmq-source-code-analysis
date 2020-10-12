/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 消息失败策略
 */
public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();
    // Broker 故障延迟机制
    private boolean sendLatencyFaultEnable = false;
    //根据currentLatency本地消息发送延迟,从latencyMax尾部向前找到第一个比currentLatency小的索引,如果没有找到,返回0
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    //根据这个索引从notAvailableDuration取出对应的时间,在该时长内,Broker设置为不可用
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * 按照上一个 broker 和 topic 消息选择出一个消息队列来发送消息
     * 1、如果开启了 sendLatencyFaultEnable 则会先找到一个消息队列，然后判断这个消息队列所在的 Broker 是不是可用的，如果可以则直接返回这个消息队列。
     * 2、如果找不到一个可用的 Broker 只能找一个相对可用的 Broker，判断这个 Broker 支不支持写入，支持就返回它。
     * 3、不支持则顺序找下一个消息队列。
     * @param tpInfo topic 消息
     * @param lastBrokerName 上一次发送消息的 broker
     * @return 消息队列
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        // 如果开启了 Broker 延迟容错机制
        if (this.sendLatencyFaultEnable) {
            try {
                // 更新 index ，这是选择消息队伍的计数器，每选一次都 + 1,后面通过对 index 取模当做消息队列数组的下标获取消息队列对象
                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                // 用来实现轮询效果
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    // 如果 pos < 0 则取第一个消息队列
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    // 获取消息队列
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    // 通过消息队列对应的 BrokerName 判断是否可用
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        // 上一个 BrokerName 为空（说明是第一次发送）or 当前消息队列的 BrokerName 等于上一个 BrokerName （只有一个 Broker）则返回该队列。
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                            return mq;
                    }
                }
                //说明现在 Broker 都是不可用的，只能到从所有 Broker 中选择一个相对可用的 Broker 并且是可写的。
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                //获得 Broker 的写队列数
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                // 写队列 > 0 说明是支持写入的
                if (writeQueueNums > 0) {
                    //获得一个消息队列,给这个消息队列指定 brokerName 和队列ID 并返回
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    // 这个 broker 不支持写入就把它删除了
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }
            // 前面都没找到就找下一个消息队列
            return tpInfo.selectOneMessageQueue();
        }
        // 没有开启 Broker 的故障延迟机制则直接选下一个 Broker
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * 通过这个实现消息延迟发送的机制
     * @param brokerName BrokerName
     * @param currentLatency 本次消息发送延迟时间
     * @param isolation 是否隔离
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        // 如果开启了 Broker 延迟机制
        if (this.sendLatencyFaultEnable) {
            // 计算 broker 不可用时长，最多 600000 ms
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            //更新当前 Broker 不可用时长
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    /**
     *
     * 消息延迟发送只支持以下 7 个时间：
     *         latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L}
     * 计算原理就是从 latencyMax 数组尾部向前遍历，找到第一个比 currentLatency 小的时间
     * 找到后会获取这个数对应的下标，然后到 notAvailableDuration 数组找到下标对应的时间。
     * @param currentLatency
     * @return
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        //从 latencyMax 尾部向前遍历
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            //找到第一个比 currentLatency 小的 latencyMax 值
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
