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

import io.altchain.data.prototube.Deserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class KafkaIterator<T> implements Iterator<T> {
  private static final int CONSUMER_WAIT_TIME = 1000;
  private static final int DEFAULT_KAFKA_CONSUMER_POLL_TIMEOUT = 1000;

  private final Deserializer<T> deserializer;
  private final KafkaPartition kafkaPartition;
  private final Consumer<ByteBuffer, ByteBuffer> kafkaConsumer;

  private ConsumerRecords<ByteBuffer, ByteBuffer> currentRecords;
  private Iterator<ConsumerRecord<ByteBuffer, ByteBuffer>> currentIterator;
  private ConsumerRecord<ByteBuffer, ByteBuffer> consumerRecord;
  private final long startOffset;
  private final long endOffset;
  private boolean outOfRange = false;

  public KafkaIterator(KafkaPartition kafkaPartition, String bootstrapServers, Deserializer<T> deserializer) {
    this.kafkaPartition = kafkaPartition;
    this.startOffset = kafkaPartition.offsetRange()._1();
    this.endOffset = kafkaPartition.offsetRange()._2();
    if (this.endOffset <= this.startOffset) {
      this.outOfRange = true;
      this.kafkaConsumer = null;
    } else {
      this.kafkaConsumer = getKafkaConsumer(bootstrapServers);
    }
    this.deserializer = deserializer;
    this.currentRecords = null;
    this.currentIterator = null;
    this.consumerRecord = null;
  }

  private Consumer<ByteBuffer, ByteBuffer> getKafkaConsumer(String bootstrapServers) {
    Consumer<ByteBuffer, ByteBuffer> kafkaConsumer = Utils.createConsumer(bootstrapServers);
    // assign will override old topic partition assignments/topic subscriptions.
    TopicPartition tp = new TopicPartition(this.kafkaPartition.topic(), this.kafkaPartition
            .partitionId());
    kafkaConsumer.assign(Collections.singletonList(tp));
    kafkaConsumer.seek(tp, startOffset);
    return kafkaConsumer;
  }

  @Override
  public boolean hasNext() {
    if (outOfRange) {
      return false;
    }
    // This will block if end offset is larger than the largest offset.
    while (currentRecords == null || currentRecords.isEmpty() || !currentIterator.hasNext()) {
      currentRecords = this.kafkaConsumer.poll(Duration.ofMillis(DEFAULT_KAFKA_CONSUMER_POLL_TIMEOUT));
      currentIterator = this.currentRecords.iterator();
      if (!currentIterator.hasNext()) {
        try {
          Thread.sleep(CONSUMER_WAIT_TIME);
        } catch (InterruptedException e) {
          // Do nothing.
        }
      }
    }
    consumerRecord = null;
    while (currentIterator.hasNext()) {
      ConsumerRecord<ByteBuffer, ByteBuffer> next = currentIterator.next();
      // In case seek to a position prior than the start offset.
      if (next.offset() < startOffset) {
        continue;
      }
      // Skip offset larger than or equals to end offset.
      // Should be taken care by next() method, putting here
      // just in case.
      if (next.offset() >= endOffset) {
        outOfRange = true;
        return false;
      }
      // Valid case.
      if (next.offset() >= startOffset) {
        consumerRecord = next;
        break;
      }
    }
    return consumerRecord != null;
  }

  @Override
  public T next() {
    if (outOfRange) {
      throw new NoSuchElementException();
    }
    if (consumerRecord.offset() >= endOffset - 1) {
      // Out of range after returned the last msg in the range.
      outOfRange = true;
    }
    return deserializer.deserialize(consumerRecord.value());
  }

}
