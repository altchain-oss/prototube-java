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

import org.apache.spark.Partition;
import scala.Tuple2;

public class KafkaPartition implements Partition {
  private static final long serialVersionUID = 1L;
  private final String topic;
  private final int partitionId;
  // offsetRange is the kafka partition offset range (start_offset,end_offset] to go over.
  private final Tuple2<Long, Long> offsetRange;
  private final int index;

  public KafkaPartition(String topic, int partitionId, Tuple2<Long, Long> offsetRange, int index) {
    this.topic = topic;
    this.partitionId = partitionId;
    this.offsetRange = offsetRange;
    this.index = index;
  }

  public String topic() {
    return topic;
  }

  public int partitionId() {
    return partitionId;
  }

  public Tuple2<Long, Long> offsetRange() {
    return offsetRange;
  }

  @Override
  public int index() {
    return index;
  }

  @Override
  public String toString() {
    String toPrint = String
        .format("{topic: %s, partitionId: %d, offsetRange: [%d, %d), index: %d}", topic, partitionId, offsetRange
            ._1(), offsetRange._2(), index);
    return toPrint;
  }
}
