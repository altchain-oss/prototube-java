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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public final class KafkaTestUtil {
  private KafkaTestUtil() {
  }

  static int getAvailablePort() {
    try {
      try (ServerSocket socket = new ServerSocket(0)) {
        return socket.getLocalPort();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to find available port to use", e);
    }
  }

  private static Admin createAdminClient(String brokerUri) {
    Properties prop = new Properties();
    prop.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUri);
    return Admin.create(prop);
  }

  public static void createKafkaTopicIfNecessary(String brokerUri, int replFactor, int numPartitions, String topic) {
    final NewTopic newTopic = new NewTopic(topic, numPartitions, (short) replFactor);

    try (Admin adminClient = createAdminClient(brokerUri)) {
      adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
