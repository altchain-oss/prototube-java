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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

class ByteBufferInputStream extends InputStream {
  private static final int BYTE_MASK = 0xff;
  private ByteBuffer buffer;

  ByteBufferInputStream(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public int read() throws IOException {
    if (buffer == null || buffer.remaining() == 0) {
      cleanUp();
      return -1;
    } else {
      return buffer.get() & BYTE_MASK;
    }
  }

  @Override
  public int read(byte[] dest) {
    return read(dest, 0, dest.length);
  }

  @Override
  public int read(byte[] dest, int offset, int length) {
    if (buffer == null || buffer.remaining() == 0) {
      cleanUp();
      return -1;
    } else {
      int amountToGet = Math.min(buffer.remaining(), length);
      buffer.get(dest, offset, amountToGet);
      return amountToGet;
    }
  }

  @Override
  public long skip(long bytes) {
    if (buffer != null) {
      int amountToSkip = (int) Math.min(bytes, buffer.remaining());
      buffer.position(buffer.position() + amountToSkip);
      if (buffer.remaining() == 0) {
        cleanUp();
      }
      return amountToSkip;
    } else {
      return 0L;
    }
  }

  /**
   * Clean up the buffer, and potentially dispose of it using StorageUtils.dispose().
   */
  private void cleanUp() {
    if (buffer != null) {
      buffer = null;
    }
  }
}
