/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.altchain.data.prototube.spark.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

final class Utils {
  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  private static final int MAX_FETCH_BYTES = 50 * 1024 * 1024;

  private Utils() {
    // Not called
  }

  static Consumer<ByteBuffer, ByteBuffer> createConsumer(String bootstrapServers) {
    final Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-ingestion-prod");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, MAX_FETCH_BYTES);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class.getName());
    // Create the consumer using props.
    return new KafkaConsumer<>(props);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  static List<TopicPartition> getTopPartitionList(Consumer consumer, String topic) {
    List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
    List<TopicPartition> topicPartitions = new ArrayList<>();
    for (PartitionInfo partitionInfo : partitionInfos) {
      topicPartitions.add(new TopicPartition(topic, partitionInfo.partition()));
    }
    return topicPartitions;
  }

  static Map<Integer, Long> getCheckpointMap(Map<String, String> checkpointMap) {
    Map<Integer, Long> newCheckpointMap = new HashMap<Integer, Long>();
    for (Entry<String, String> e : checkpointMap.entrySet()) {
      newCheckpointMap.put(Integer.valueOf(e.getKey()), Long.valueOf(e.getValue()));
    }
    LOG.info("Read previous checkpoints from checkpoint store: {}",
             Arrays.toString(newCheckpointMap.entrySet().toArray()));
    return newCheckpointMap;
  }

  // Merge previous checkpoint map with new beginning offset map.
  static Map<Integer, Long> getStartOffsets(Map<Integer, Long> prevCheckpointMap,
      Map<TopicPartition, Long> beginningOffsets) {
    Map<Integer, Long> checkpointMap = new HashMap<>(prevCheckpointMap);
    for (Entry<TopicPartition, Long> p : beginningOffsets.entrySet()) {
      TopicPartition tp = p.getKey();
      int partition = tp.partition();
      if (!checkpointMap.containsKey(partition)
          || checkpointMap.get(partition) < beginningOffsets.get(tp)) {
        // Ensure the smallest offset is scanned
        checkpointMap.put(partition, beginningOffsets.get(tp));
      }
    }
    LOG.info("Got Kafka scan start offsets: {}", Arrays.toString(checkpointMap.entrySet().toArray()));
    return checkpointMap;
  }

  static Map<String, String> getNewCheckpoint(Map<TopicPartition, Long> endOffsets) {
    Map<String, String> checkpointMap = new HashMap<>();
    for (Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
      checkpointMap.put(String.valueOf(entry.getKey().partition()), entry.getValue().toString());
    }
    LOG.info("Computed new checkpoints to save to checkpoint store: {}",
             Arrays.toString(checkpointMap.entrySet().toArray()));
    return checkpointMap;
  }
}
