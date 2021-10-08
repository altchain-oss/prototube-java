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

package io.altchain.data.prototube.flink;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.altchain.data.prototube.MessageSerializer;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

class RowDataSerializationSchema implements SerializationSchema<RowData> {
  private static final long serialVersionUID = 1L;
  private static final int MILLI_TO_SECOND = 1000;
  private final String payloadClassName;
  private final RowType rowType;

  private transient Map<Integer, Descriptors.FieldDescriptor> descriptors;
  private transient Method newBuilder;

  RowDataSerializationSchema(String payloadClassName, RowType rowType) {
    this.payloadClassName = payloadClassName;
    this.rowType = rowType;
  }

  @Override
  public void open(InitializationContext context) throws Exception {
    SerializationSchema.super.open(context);
    Class<?> payloadClass = context.getUserCodeClassLoader().asClassLoader().loadClass(payloadClassName);
    newBuilder = payloadClass.getMethod("newBuilder");

    Descriptors.Descriptor desc = (Descriptors.Descriptor) payloadClass.getMethod("getDescriptor").invoke(null);
    descriptors = RowDataSerializationSchema.resolveFieldMapping(rowType, desc);
  }

  @Override
  public byte[] serialize(RowData e) {
    AbstractMessage.Builder<?> record;
    try {
      record = (AbstractMessage.Builder<?>) newBuilder.invoke(null);
    } catch (IllegalAccessException | InvocationTargetException ex) {
      throw new RuntimeException(ex);
    }
    for (int i = 0; i < rowType.getFieldCount(); ++i) {
      Object o = getRecordValue(e, i);
      record.setField(descriptors.get(i), o);
    }
    long time = System.currentTimeMillis() / MILLI_TO_SECOND;
    return MessageSerializer.serialize(record.build(), time);
  }

  private Object getRecordValue(RowData row, int idx) {
    switch (rowType.getTypeAt(idx).getTypeRoot()) {
      case CHAR:
      case VARCHAR:
        return row.getString(idx).toString();
      case BOOLEAN:
        return row.getBoolean(idx);
      case BINARY:
      case VARBINARY:
        return ByteString.copyFrom(row.getBinary(idx));
      case DECIMAL:
      case RAW:
      case DISTINCT_TYPE:
      case ROW:
      case STRUCTURED_TYPE:
      case MULTISET:
      case MAP:
      case ARRAY:
        throw new UnsupportedOperationException("Unimplemented");
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
        LogicalType fieldType = rowType.getTypeAt(idx);
        final int timestampPrecision = getPrecision(fieldType);
        return row.getTimestamp(idx, timestampPrecision).getMillisecond() / MILLI_TO_SECOND;
      }
      case TINYINT:
        return row.getByte(idx);
      case SMALLINT:
        return row.getShort(idx);
      case INTEGER:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
      case INTERVAL_YEAR_MONTH:
        return row.getInt(idx);
      case BIGINT:
      case INTERVAL_DAY_TIME:
        return row.getLong(idx);
      case FLOAT:
        return row.getFloat(idx);
      case DOUBLE:
        return row.getDouble(idx);
      case TIMESTAMP_WITH_TIME_ZONE:
        throw new UnsupportedOperationException();
      case NULL:
      case SYMBOL:
      case UNRESOLVED:
      default:
        throw new IllegalArgumentException();
    }
  }

  static Map<Integer, Descriptors.FieldDescriptor> resolveFieldMapping(
      RowType rowType, Descriptors.Descriptor descriptors) {
    HashMap<String, Descriptors.FieldDescriptor> fields = new HashMap<>();
    for (Descriptors.FieldDescriptor f : descriptors.getFields()) {
      fields.put(f.getName(), f);
    }
    HashMap<Integer, Descriptors.FieldDescriptor> res = new HashMap<>();
    for (int i = 0; i < rowType.getFieldCount(); ++i) {
      RowType.RowField f = rowType.getFields().get(i);
      Descriptors.FieldDescriptor fd = fields.get(f.getName());
      if (fd == null) {
        throw new IllegalArgumentException();
      }
      res.put(i, fd);
    }
    return res;
  }
}
