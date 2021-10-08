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

import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

public class JsonDeserializer extends MessageDeserializer implements Deserializer<String> {
  private static final long serialVersionUID = 0L;

  public JsonDeserializer(String payloadClassName) throws NoSuchMethodException, InvocationTargetException,
          IllegalAccessException, ClassNotFoundException {
    super(payloadClassName);
  }

  public String deserialize(ByteBuffer buf) {
    try {
      boolean r = deserializeMessage(buf);
      if (!r) {
        return null;
      }
      return JsonFormat.printer().print(record);
    } catch (IOException e) {
      return null;
    }
  }
}