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

package io.altchain.data.prototube.testutils.kafka;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.altchain.data.prototube.idl.Example;
import io.altchain.data.prototube.kafka.Producer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

public class ProducerTest {
  private static final Random RANDOM = new Random(System.currentTimeMillis());
  private static final int NUM_TEST_TOPIC_PARTITIONS = 1;
  private static final String TEST_TOPIC = "test_topic";
  private static MiniKafkaCluster kafkaCluster;

  @BeforeClass
  public static void setup() throws Exception {
    kafkaCluster = new MiniKafkaCluster.Builder().newServer("0").build();
    kafkaCluster.start();
    String brokerAddress = getKafkaBroker();
    KafkaTestUtil.createKafkaTopicIfNecessary(brokerAddress, 1, NUM_TEST_TOPIC_PARTITIONS, TEST_TOPIC);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    kafkaCluster.close();
  }

  @Test
  public void testProduceMessages() {
    try (Producer producer = new Producer(TEST_TOPIC, getKafkaBroker())) {
      for (int i = 0; i < 100; i++) {
        producer.emit(generateRandomEvent());
      }
      producer.flush();
    }
  }

  private static String getKafkaBroker() {
    return "127.0.0.1:" + kafkaCluster.getKafkaServerPort(0);
  }

  private static Message generateRandomEvent() {
    return Example.ExamplePrototubeMessage.newBuilder().setInt32Field(RANDOM.nextInt()).setInt64Field(RANDOM.nextLong())
        .setDoubleField(RANDOM.nextDouble()).setStringField(UUID.randomUUID().toString())
        .setBytesField(ByteString.copyFromUtf8(UUID.randomUUID().toString())).build();
  }
}
