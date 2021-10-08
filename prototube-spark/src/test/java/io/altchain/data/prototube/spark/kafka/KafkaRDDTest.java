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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.altchain.data.prototube.spark.CheckpointStore;
import io.altchain.data.prototube.spark.RecordDeserializer;
import io.altchain.data.prototube.testutils.kafka.KafkaTestUtil;
import io.altchain.data.prototube.testutils.kafka.MiniKafkaCluster;
import org.apache.commons.collections.IteratorUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConversions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class KafkaRDDTest {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaRDDTest.class);

  private static final long STABILIZE_SLEEP_DELAYS = 3000;
  private static final String TEST_TOPIC_1 = "foo";
  private static final String TEST_TOPIC_2 = "bar";
  private static final int NUM_TEST_TOPIC_PARTITIONS = 2;
  private static final int NUM_MSG_TO_PRODUCE = 250;

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static MiniKafkaCluster kafkaCluster;
  private static MapCheckpointStore checkpointStore;
  private static SparkContext mockedSparkContext;
  private static RecordDeserializer deserializer;
  private static final List<List<String>> EXPECTED_RESULTS_FOR_TOPIC_FOO = new ArrayList<>();

  @BeforeClass
  public static void setup() throws Exception {
    checkpointStore = new MapCheckpointStore();
    checkpointStore.open();
    kafkaCluster = new MiniKafkaCluster.Builder().newServer("0").build();
    LOG.info("Trying to start MiniKafkaCluster");
    kafkaCluster.start();
    String zkAddress = getZkAddress();
    String brokerAddress = getKafkaBroker();

    KafkaTestUtil.createKafkaTopicIfNecessary(getKafkaBroker(), 1, NUM_TEST_TOPIC_PARTITIONS, TEST_TOPIC_1);
    KafkaTestUtil.createKafkaTopicIfNecessary(getKafkaBroker(), 1, NUM_TEST_TOPIC_PARTITIONS, TEST_TOPIC_2);
    // Wait until Kafka / ZK stabilizes
    Thread.sleep(STABILIZE_SLEEP_DELAYS);

    for (int i = 0; i < NUM_TEST_TOPIC_PARTITIONS; i++) {
      EXPECTED_RESULTS_FOR_TOPIC_FOO.add(new ArrayList<>());
    }
    try (KafkaProducer<byte[], byte[]> producer = getProducer(brokerAddress)) {
      for (int i = 0; i < NUM_MSG_TO_PRODUCE; ++i) {
        byte[] payloadBytes = MAPPER.writeValueAsBytes(ImmutableMap.of(TEST_TOPIC_1, i));
        producer.send(new ProducerRecord<>(TEST_TOPIC_1, Integer.toString(i % 2).getBytes(), payloadBytes));
        EXPECTED_RESULTS_FOR_TOPIC_FOO.get(i % NUM_TEST_TOPIC_PARTITIONS).add(String.format("[%s]", new String(
            payloadBytes)));
      }
    }

    mockedSparkContext = mock(SparkContext.class);
    doReturn("kafka-ingestion").when(mockedSparkContext).appName();
    doReturn("kafka-ingestion").when(mockedSparkContext).applicationId();
    doReturn("local").when(mockedSparkContext).master();
    SparkConf sparkConf = new SparkConf();
    doReturn(sparkConf).when(mockedSparkContext).conf();
    doReturn("test-version").when(mockedSparkContext).version();

    deserializer = mock(RecordDeserializer.class);
    doAnswer((Answer<Row>) invocationOnMock -> {
      ByteBuffer buf = invocationOnMock.getArgumentAt(0, ByteBuffer.class);
      byte[] b = new byte[buf.remaining()];
      buf.get(b);
      return RowFactory.create(new String(b, UTF_8));
    }).when(deserializer).deserialize(any());
  }

  @AfterClass
  public static void shutDown() throws Exception {
    kafkaCluster.close();
    checkpointStore.close();
  }

  private static String getZkAddress() {
    return "127.0.0.1:" + kafkaCluster.getZkServer().getPort();
  }

  private static String getKafkaBroker() {
    return "127.0.0.1:" + kafkaCluster.getKafkaServerPort(0);
  }

  private static KafkaProducer<byte[], byte[]> getProducer(String brokerList) {
    Properties prop = new Properties();
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    return new KafkaProducer<>(prop);
  }

  @Test
  public void testKafkaRowIterator() {
    int cnt;

    // smallestOffset = 0, largestOffset = NUM_MSG_TO_PRODUCE / 2 - 1= 124;
    int smallestOffset = 0;
    int largestOffset = NUM_MSG_TO_PRODUCE / 2;

    // Test smallestOffset = startOffset < endOffset = largestOffset.
    cnt = getIteratorLength(TEST_TOPIC_1, smallestOffset, largestOffset);
    Assert.assertEquals(NUM_MSG_TO_PRODUCE / 2, cnt);

    // Test smallestOffset < startOffset < endOffset = largestOffset.
    cnt = getIteratorLength(TEST_TOPIC_1, smallestOffset + 1, largestOffset);
    Assert.assertEquals(NUM_MSG_TO_PRODUCE / 2 - 1, cnt);

    // Test smallestOffset < startOffset = endOffset < largestOffset.
    cnt = getIteratorLength(TEST_TOPIC_1, smallestOffset + 5, smallestOffset + 5);
    Assert.assertEquals(0, cnt);

    // Test startOffset = smallestOffset < endOffset < largestOffset.
    cnt = getIteratorLength(TEST_TOPIC_1, smallestOffset, smallestOffset + 1);
    Assert.assertEquals(1, cnt);

    // Test smallestOffset < startOffset < endOffset < largestOffset.
    cnt = getIteratorLength(TEST_TOPIC_1, smallestOffset + 10, smallestOffset + 11);
    Assert.assertEquals(1, cnt);

    // Test smallestOffset < startOffset < endOffset < largestOffset.
    cnt = getIteratorLength(TEST_TOPIC_1, smallestOffset + 4, largestOffset - 4);
    Assert.assertEquals(NUM_MSG_TO_PRODUCE / 2 - 8, cnt);

    // Test smallestOffset < startOffset < largestOffset < endOffset.
    cnt = getIteratorLength(TEST_TOPIC_1, smallestOffset + 4, largestOffset);
    Assert.assertEquals(NUM_MSG_TO_PRODUCE / 2 - 4, cnt);

    // Test startOffset = (largestOffset - 2) < endOffset.
    cnt = getIteratorLength(TEST_TOPIC_1, largestOffset - 2, largestOffset);
    Assert.assertEquals(2, cnt);

    // Test startOffset = (largestOffset - 1) < endOffset.
    cnt = getIteratorLength(TEST_TOPIC_1, largestOffset - 1, largestOffset);
    Assert.assertEquals(1, cnt);

    // Test startOffset = largestOffset < endOffset.
    cnt = getIteratorLength(TEST_TOPIC_1, largestOffset, largestOffset);
    Assert.assertEquals(0, cnt);
  }

  @Test
  public void testKafkaRowIteratorWithEmptyTopic() {
    int cnt;
    cnt = getIteratorLength(TEST_TOPIC_2, 0, 0);
    Assert.assertEquals(0, cnt);
  }

  private int getIteratorLength(String topic, long startOffset, long endOffset) {
    Tuple2<Long, Long> range = new Tuple2<>(startOffset, endOffset);
    KafkaPartition kp = new KafkaPartition(topic, 0, range, 0);
    KafkaIterator<Row> it = new KafkaIterator<>(kp, getKafkaBroker(), deserializer);
    int cnt = 0;
    while (it.hasNext()) {
      it.next();
      cnt++;
    }
    return cnt;
  }

  @Test
  public void testKafkaRDDInBootstrapMode() {
    Consumer<ByteBuffer, ByteBuffer> kafkaConsumer = Utils.createConsumer(getKafkaBroker());
    List<TopicPartition> topicPartitions = Utils.getTopPartitionList(kafkaConsumer, TEST_TOPIC_1);
    Map<String, String> checkpoints = new HashMap<>();
    Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(topicPartitions);

    // Bootstrap mode, no check point.
    checkpointStore.saveCheckpoint(TEST_TOPIC_1, checkpoints);
    Map<Integer, Long> startOffsets = Utils.getStartOffsets(Utils.getCheckpointMap(checkpointStore
        .getCheckpoint(TEST_TOPIC_1)), kafkaConsumer.beginningOffsets(topicPartitions));
    KafkaRDD rdd = new KafkaRDD(mockedSparkContext, getKafkaBroker(), deserializer, startOffsets, endOffsets);

    for (Partition p : rdd.getPartitions()) {
      List<Row> rows = iterator2List(rdd.compute(p, null));
      Assert.assertEquals(NUM_MSG_TO_PRODUCE / 2, rows.size());
      String[] expectedArray = EXPECTED_RESULTS_FOR_TOPIC_FOO.get(((KafkaPartition) p).partitionId())
          .toArray(new String[0]);
      String[] actualArray = convertRowsToStrings(rows);
      Assert.assertArrayEquals(expectedArray, actualArray);
    }
  }

  @Test
  public void testKafkaRDDWithCheckpoint() {
    Consumer<ByteBuffer, ByteBuffer> kafkaConsumer = Utils.createConsumer(getKafkaBroker());
    List<TopicPartition> topicPartitions = Utils.getTopPartitionList(kafkaConsumer, TEST_TOPIC_1);
    Map<String, String> checkpoints = new HashMap<>();
    Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(topicPartitions);

    // Reset checkpoints
    for (int i = 0; i <= NUM_MSG_TO_PRODUCE / 2; ++i) {
      checkpoints.clear();
      checkpoints.put("0", String.valueOf(i));
      checkpoints.put("1", String.valueOf(i));
      checkpointStore.saveCheckpoint(TEST_TOPIC_1, checkpoints);
      Map<Integer, Long> startOffsets = Utils.getStartOffsets(Utils.getCheckpointMap(checkpointStore
          .getCheckpoint(TEST_TOPIC_1)), kafkaConsumer.beginningOffsets(topicPartitions));
      KafkaRDD rdd = new KafkaRDD(mockedSparkContext, getKafkaBroker(), deserializer, startOffsets, endOffsets);
      for (Partition p : rdd.getPartitions()) {
        List<Row> rows = iterator2List(rdd.compute(p, null));
        Assert.assertEquals(NUM_MSG_TO_PRODUCE / 2 - i, rows.size());
        String[] expectedArray = EXPECTED_RESULTS_FOR_TOPIC_FOO.get(((KafkaPartition) p).partitionId())
            .subList(i, NUM_MSG_TO_PRODUCE / 2)
            .toArray(new String[0]);
        String[] actualArray = convertRowsToStrings(rows);
        Assert.assertArrayEquals(expectedArray, actualArray);
      }
    }
  }

  @Test
  public void testKafkaRDDWithTopicExpansion() {
    Consumer<ByteBuffer, ByteBuffer> kafkaConsumer = Utils.createConsumer(getKafkaBroker());
    List<TopicPartition> topicPartitions = Utils.getTopPartitionList(kafkaConsumer, TEST_TOPIC_1);
    Map<String, String> checkpoints = new HashMap<>();
    Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(topicPartitions);
    // Test Topic Expansion situation (only 1 partition checkpoint in checkpoint store.)
    for (int i = 0; i <= NUM_MSG_TO_PRODUCE / 2; ++i) {
      checkpoints.clear();
      checkpoints.put("0", String.valueOf(i));
      checkpointStore.saveCheckpoint(TEST_TOPIC_1, checkpoints);
      Map<Integer, Long> startOffsets = Utils.getStartOffsets(Utils.getCheckpointMap(checkpointStore
          .getCheckpoint(TEST_TOPIC_1)), kafkaConsumer.beginningOffsets(topicPartitions));
      KafkaRDD rdd = new KafkaRDD(mockedSparkContext, getKafkaBroker(), deserializer, startOffsets, endOffsets);
      for (Partition p : rdd.getPartitions()) {
        List<Row> rows = iterator2List(rdd.compute(p, null));
        if (p.index() == 0) {
          Assert.assertEquals(NUM_MSG_TO_PRODUCE / 2 - i, rows.size());
          String[] expectedArray = EXPECTED_RESULTS_FOR_TOPIC_FOO.get(0).subList(i, NUM_MSG_TO_PRODUCE / 2)
              .toArray(new String[0]);
          String[] actualArray = convertRowsToStrings(rows);
          Assert.assertArrayEquals(expectedArray, actualArray);
        } else {
          Assert.assertEquals(NUM_MSG_TO_PRODUCE / 2, rows.size());
          String[] expectedArray = EXPECTED_RESULTS_FOR_TOPIC_FOO.get(1).toArray(new String[0]);
          String[] actualArray = convertRowsToStrings(rows);
          Assert.assertArrayEquals(expectedArray, actualArray);
        }
      }
    }
  }

  @Test
  public void testKafkaRDDWithEmptyKafkaTopic() {
    Consumer<ByteBuffer, ByteBuffer> kafkaConsumer = Utils.createConsumer(getKafkaBroker());
    List<TopicPartition> topicPartitions = Utils.getTopPartitionList(kafkaConsumer, TEST_TOPIC_2);
    Map<String, String> checkpoints = new HashMap<>();
    checkpointStore.saveCheckpoint(TEST_TOPIC_2, checkpoints);
    Map<Integer, Long> startOffsets = Utils.getStartOffsets(Utils.getCheckpointMap(checkpointStore
        .getCheckpoint(TEST_TOPIC_2)), kafkaConsumer.beginningOffsets(topicPartitions));
    Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(topicPartitions);
    KafkaRDD rdd = new KafkaRDD(mockedSparkContext, getKafkaBroker(), deserializer, startOffsets, endOffsets);
    for (Partition p : rdd.getPartitions()) {
      List<Row> rows = iterator2List(rdd.compute(p, null));
      Assert.assertEquals(0, rows.size());
    }
  }

  @SuppressWarnings("unchecked")
  private List<Row> iterator2List(Iterator<Row> it) {
    return IteratorUtils.toList(JavaConversions.asJavaIterator(it));
  }

  private String[] convertRowsToStrings(List<Row> rows) {
    String[] actualArray = new String[rows.size()];
    for (int i = 0; i < rows.size(); i++) {
      actualArray[i] = rows.get(i).toString();
    }
    return actualArray;
  }

  static class MapCheckpointStore implements CheckpointStore {
    private final Map<String, Map<String, String>> checkpointMap = new HashMap<>();

    @Override
    public void close() {
      // Do nothing
    }

    @Override
    public void open() {
      // Do nothing
    }

    @Override
    public Map<String, String> getCheckpoint(String stream) {
      return checkpointMap.get(stream);
    }

    @Override
    public void saveCheckpoint(String stream, Map<String, String> checkpoints) {
      checkpointMap.put(stream, checkpoints);
    }
  }

}
