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

package io.altchain.data.prototube;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import io.altchain.data.prototube.idl.Prototube;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

public final class MessageSerializer {
  private static final int VERSION = 1;
  private static final int UUID_LENGTH = 16;
  private static final Prototube.PrototubeMessageHeader HEADER_TEMPLATE = Prototube.PrototubeMessageHeader.newBuilder()
      .setVersion(VERSION)
      .setTs(0)
      .setUuid(ByteString.copyFrom(new byte[UUID_LENGTH]))
      .buildPartial();
  private static final int HEADER_SIZE = getDelimitedMessageSize(HEADER_TEMPLATE);

  private MessageSerializer() {
  }

  public static byte[] serialize(Message message, long time) {
    UUID uuid = UUID.randomUUID();
    ByteBuffer bb = ByteBuffer.wrap(new byte[UUID_LENGTH]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    bb.flip();

    Prototube.PrototubeMessageHeader header = Prototube.PrototubeMessageHeader.newBuilder(HEADER_TEMPLATE)
        .setTs(time)
        .setUuid(ByteString.copyFrom(bb))
        .build();
    ByteArrayOutputStream os = new ByteArrayOutputStream(getRecordSize(message));
    try {
      os.write(MessageDeserializer.HEADER);
      header.writeDelimitedTo(os);
      message.writeDelimitedTo(os);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return os.toByteArray();
  }

  private static int getRecordSize(Message message) {
    return MessageDeserializer.HEADER.length + HEADER_SIZE + getDelimitedMessageSize(message);
  }

  private static int getDelimitedMessageSize(Message msg) {
    int serialized = msg.getSerializedSize();
    return CodedOutputStream.computeUInt32SizeNoTag(serialized) + serialized;
  }
}
