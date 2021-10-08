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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import io.altchain.data.prototube.spark.RecordDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

public class KafkaRDD extends RDD<Row> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(KafkaRDD.class);
  private static final ClassTag<Row> CLASS_TAG = ClassManifestFactory$.MODULE$.classType(Row.class);

  private final String bootstrapServers;
  private final String recordClassName;
  private final RecordDeserializer recordDeserializer;
  private final Map<Integer, Long> startOffsets;
  private final Map<TopicPartition, Long> endOffsets;

  public KafkaRDD(SparkContext sparkContext,
                  String bootstrapServers,
                  String recordClassName,
                  Map<Integer, Long> startOffsets,
                  Map<TopicPartition, Long> endOffsets) {
    super(sparkContext, new ArrayBuffer<>(), CLASS_TAG);
    this.bootstrapServers = bootstrapServers;
    this.recordClassName = recordClassName;
    this.recordDeserializer = null;
    this.startOffsets = Collections.unmodifiableMap(startOffsets);
    this.endOffsets = Collections.unmodifiableMap(endOffsets);
  }

  @VisibleForTesting
  public KafkaRDD(SparkContext sparkContext,
                  String bootstrapServers,
                  RecordDeserializer recordDeserializer,
                  Map<Integer, Long> startOffsets,
                  Map<TopicPartition, Long> endOffsets) {
    super(sparkContext, new ArrayBuffer<>(), CLASS_TAG);
    this.bootstrapServers = bootstrapServers;
    this.recordClassName = null;
    this.recordDeserializer = recordDeserializer;
    this.startOffsets = Collections.unmodifiableMap(startOffsets);
    this.endOffsets = Collections.unmodifiableMap(endOffsets);
  }

  @Override
  public Iterator<Row> compute(Partition split, TaskContext context) {
    KafkaPartition kp = (KafkaPartition) split;
    RecordDeserializer deserializer = recordDeserializer;
    if (recordClassName != null) {
      try {
        deserializer = new RecordDeserializer(recordClassName);
      } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    KafkaIterator<Row> it = new KafkaIterator<>(
        kp,
        bootstrapServers,
        deserializer);
    UnmodifiableIterator<Row> filtered = Iterators.filter(it, Objects::nonNull);
    return JavaConversions.asScalaIterator(filtered);
  }

  @Override
  public Partition[] getPartitions() {
    Partition[] kafkaPartitions = new KafkaPartition[this.endOffsets.size()];
    int idx = 0;
    for (Entry<TopicPartition, Long> entry : this.endOffsets.entrySet()) {
      int partitionId = entry.getKey().partition();
      Tuple2<Long, Long> offsetRange = new Tuple2<Long, Long>(startOffsets.get(partitionId), entry.getValue());
      KafkaPartition kafkaPartition = new KafkaPartition(entry.getKey().topic(), partitionId, offsetRange, idx);
      kafkaPartitions[idx++] = kafkaPartition;
    }
    LOG.info("Got KafkaPartition scan range : {}", Arrays.toString(kafkaPartitions));
    return kafkaPartitions;
  }

  public Dataset<Row> toDataFrame(SQLContext sqlContext) {
    JavaRDD<Row> rdd = toJavaRDD();
    try {
      StructType ty = new RecordDeserializer(recordClassName).schema();
      return sqlContext.createDataFrame(rdd, ty);
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
