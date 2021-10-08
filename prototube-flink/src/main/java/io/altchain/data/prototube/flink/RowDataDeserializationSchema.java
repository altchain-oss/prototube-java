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
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import io.altchain.data.prototube.MessageDeserializer;
import io.altchain.data.prototube.idl.Prototube;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class RowDataDeserializationSchema extends MessageDeserializer
    implements DeserializationSchema<RowData> {
  private static final long serialVersionUID = 2L;
  private static final int SECOND_TO_MILLS = 1000;
  private static final FieldSelector GET_HEADER = new FieldSelector(true);
  private static final FieldSelector GET_MSG = new FieldSelector(false);
  private static final String HEADER_PREFIX = "_pbtb_";

  private final TypeInformation<RowData> type;
  private final RowType rowType;
  private transient List<Map.Entry<FieldGetter, FieldDescriptor>> rowFields;

  private interface FieldGetter {
    Object get(MessageOrBuilder header, Object msg, Object desc);
  }

  private static final class FieldSelector implements FieldGetter {
    private final boolean isHeader;

    private FieldSelector(boolean isHeader) {
      this.isHeader = isHeader;
    }

    @Override
    public Object get(MessageOrBuilder header, Object msg, Object desc) {
      MessageOrBuilder m = isHeader ? header : (MessageOrBuilder) msg;
      return m.getField((FieldDescriptor) desc);
    }
  }

  private RowDataDeserializationSchema(
      String payloadClassName,
      Prototube.PrototubeMessageHeader.Builder header,
      AbstractMessage.Builder<?> record,
      TypeInformation<RowData> type,
      RowType rowType) {
    super(payloadClassName, header, record);
    this.type = type;
    this.rowType = rowType;
  }

  /**
   * Create a RowDataDeserializationSchema.
   */
  static DeserializationSchema<RowData> create(
      String payloadClassName, TypeInformation<RowData> type, RowType tableType)
      throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    final Prototube.PrototubeMessageHeader.Builder header = Prototube.PrototubeMessageHeader.newBuilder();
    Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(payloadClassName);
    AbstractMessage.Builder<?> record = (AbstractMessage.Builder<?>) clazz.getMethod("newBuilder").invoke(null);

    return new
        RowDataDeserializationSchema(payloadClassName, header, record, type, tableType);
  }

  @Override
  public void open(InitializationContext context) throws Exception {
    DeserializationSchema.super.open(context);
    Descriptor headerDesc = Prototube.PrototubeMessageHeader.getDescriptor();
    Descriptor desc = record.getDescriptorForType();
    rowFields = computeSnippets(rowType, headerDesc, desc);
  }

  @Override
  public RowData deserialize(byte[] message) throws IOException {
    ByteBuffer buf = ByteBuffer.wrap(message);
    boolean ret = deserializeMessage(buf);
    if (!ret) {
      return null;
    }

    Object[] f = new Object[rowType.getFieldCount()];
    for (int i = 0; i < rowType.getFieldCount(); ++i) {
      Map.Entry<FieldGetter, FieldDescriptor> e = rowFields.get(i);
      f[i] = e.getKey().get(header, record, e.getValue());
    }

    return GenericRowData.of(f);
  }

  public boolean isEndOfStream(RowData nextElement) {
    return false;
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return type;
  }

  private static List<Map.Entry<FieldGetter, FieldDescriptor>> computeSnippets(
      RowType ty, Descriptor headerDescriptor, Descriptor msgDescriptor) {
    List<Map.Entry<FieldGetter, FieldDescriptor>> seq = new ArrayList<>();
    HashMap<String, FieldDescriptor> headerFields = new HashMap<>();
    HashMap<String, FieldDescriptor> msgFields = new HashMap<>();
    if (headerDescriptor != null) {
      headerDescriptor.getFields().forEach(x -> headerFields.put(x.getName(), x));
    }
    if (msgDescriptor == null) {
      throw new IllegalArgumentException();
    }
    msgDescriptor.getFields().forEach(x -> msgFields.put(x.getName(), x));

    for (int i = 0; i < ty.getFieldCount(); ++i) {
      RowType.RowField f = ty.getFields().get(i);
      boolean isHeader = f.getName().startsWith(HEADER_PREFIX);
      final FieldDescriptor desc;
      if (isHeader) {
        desc = headerFields.get(f.getName().substring(HEADER_PREFIX.length()));
      } else {
        desc = msgFields.get(f.getName());
      }

      if (desc == null) {
        throw new IllegalArgumentException("No field " + f.getName() + " in the schema");
      }

      FieldGetter getter = computeGetter(f.getType(), isHeader ? GET_HEADER : GET_MSG, desc);
      seq.add(new HashMap.SimpleImmutableEntry<>(getter, desc));
    }
    return seq;
  }

  private static FieldGetter computeGetter(LogicalType ty, FieldGetter parent, FieldDescriptor fieldDesc) {
    switch (ty.getTypeRoot()) {
      case STRUCTURED_TYPE:
      case ROW: {
        RowType nestedType = (RowType) ty;
        List<Map.Entry<FieldGetter, FieldDescriptor>> fields =
            computeSnippets(nestedType, null, fieldDesc.getMessageType());
        return (header, msg, desc) -> {
          Message m = (Message) parent.get(header, msg, desc);
          Object[] nestedValue = new Object[nestedType.getFieldCount()];
          for (int i = 0; i < nestedType.getFieldCount(); ++i) {
            Map.Entry<FieldGetter, FieldDescriptor> e = fields.get(i);
            nestedValue[i] = e.getKey().get(null, m, e.getValue());
          }
          return GenericRowData.of(nestedValue);
        };
      }
      case ARRAY: {
        ArrayType aty = (ArrayType) ty;
        FieldGetter e = computeGetter(aty.getElementType(),
            (dummy, list, idx) -> ((List<?>) list).get((int) idx), fieldDesc);
        return (header, msg, desc) -> {
          List<?> l = (List<?>) parent.get(header, msg, desc);
          Object[] array = new Object[l.size()];
          for (int i = 0; i < l.size(); ++i) {
            array[i] = e.get(header, l, i);
          }
          return new GenericArrayData(array);
        };
      }
      default:
        return (header, msg, desc) -> {
          Object o = parent.get(header, msg, desc);
          return unwrapPrimitive(o, ty);
        };
    }
  }

  private static Object unwrapPrimitive(Object v, LogicalType ty) {
    switch (ty.getTypeRoot()) {
      case CHAR:
      case VARCHAR:
        return StringData.fromString(v.toString());
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
      case INTERVAL_YEAR_MONTH:
      case BIGINT:
      case INTERVAL_DAY_TIME:
      case FLOAT:
      case DOUBLE:
        return v;
      case BINARY:
      case VARBINARY:
        return ((ByteString) v).toByteArray();
      case DECIMAL:
      case RAW:
      case DISTINCT_TYPE:

      case MULTISET:
      case MAP:
        throw new UnsupportedOperationException("Unimplemented");
      case STRUCTURED_TYPE:
      case ROW:
      case ARRAY:
        throw new IllegalArgumentException("Unreachable");
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return TimestampData.fromEpochMillis((Long) v * SECOND_TO_MILLS);
      case TIMESTAMP_WITH_TIME_ZONE:
        throw new UnsupportedOperationException();
      case NULL:
        return null;
      case SYMBOL:
      case UNRESOLVED:
      default:
        throw new IllegalArgumentException();
    }
  }
}
