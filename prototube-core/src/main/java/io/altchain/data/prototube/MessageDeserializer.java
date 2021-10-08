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

import com.google.protobuf.AbstractMessage;
import io.altchain.data.prototube.idl.Prototube;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class MessageDeserializer implements Serializable {
  static final byte[] HEADER = "PBTB".getBytes(StandardCharsets.UTF_8);
  private static final long serialVersionUID = 0L;
  protected transient Prototube.PrototubeMessageHeader.Builder header;
  protected transient AbstractMessage.Builder<?> record;
  private final String payloadClassName;

  protected MessageDeserializer(String payloadClassName) throws
      NoSuchMethodException, InvocationTargetException,
      IllegalAccessException, ClassNotFoundException {
    this.payloadClassName = payloadClassName;
    this.header = Prototube.PrototubeMessageHeader.newBuilder();
    Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(payloadClassName);
    this.record = (AbstractMessage.Builder) clazz.getMethod("newBuilder").invoke(null);
  }

  protected MessageDeserializer(
          String payloadClassName,
          Prototube.PrototubeMessageHeader.Builder header,
          AbstractMessage.Builder<?> record) {
    this.payloadClassName = payloadClassName;
    this.header = header;
    this.record = record;
  }

  private void readObject(ObjectInputStream in)
          throws IOException, ClassNotFoundException, NoSuchMethodException,
          InvocationTargetException, IllegalAccessException {
    in.defaultReadObject();
    this.header = Prototube.PrototubeMessageHeader.newBuilder();
    Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(payloadClassName);
    this.record = (AbstractMessage.Builder) clazz.getMethod("newBuilder").invoke(null);
  }

  protected boolean deserializeMessage(ByteBuffer buf) throws IOException {
    final byte[] magicNumbers = new byte[HEADER.length];
    try (InputStream is = new ByteBufferInputStream(buf)) {
      int ret = is.read(magicNumbers);
      if (ret != HEADER.length || !Arrays.equals(magicNumbers, HEADER)) {
        return false;
      }
      header.clear().mergeDelimitedFrom(is);
      record.clear().mergeDelimitedFrom(is);
    }
    return true;
  }
}
