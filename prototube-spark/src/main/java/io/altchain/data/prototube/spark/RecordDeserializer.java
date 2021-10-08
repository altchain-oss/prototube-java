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

package io.altchain.data.prototube.spark;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.MessageOrBuilder;
import io.altchain.data.prototube.Deserializer;
import io.altchain.data.prototube.MessageDeserializer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.List;

public class RecordDeserializer extends MessageDeserializer implements Deserializer<Row> {
  private static final long serialVersionUID = 1L;
  private final StructType headerSchema;
  private final StructType recordSchema;
  private final int totalFields;

  public RecordDeserializer(String payloadClassName) throws
      NoSuchMethodException, InvocationTargetException,
      IllegalAccessException, ClassNotFoundException {
    super(payloadClassName);
    this.headerSchema = SchemaReflection.headerSchema(header.getDescriptorForType());
    this.recordSchema = SchemaReflection.schemaFor(record.getDescriptorForType());
    this.totalFields = headerSchema.size() + recordSchema.size();
  }

  public StructType schema() {
    StructField[] fields = new StructField[headerSchema.fields().length + recordSchema.fields().length];
    System.arraycopy(headerSchema.fields(), 0, fields, 0, headerSchema.fields().length);
    System.arraycopy(recordSchema.fields(), 0, fields, headerSchema.fields().length, recordSchema.fields().length);
    return new StructType(fields);
  }

  public Row deserialize(ByteBuffer buf) {
    try {
      boolean ret = deserializeMessage(buf);
      if (!ret) {
        return null;
      }
    } catch (IOException ignored) {
      return null;
    }

    Object[] fields = new Object[totalFields];
    protoToRow(header, headerSchema, fields, 0);
    protoToRow(record, recordSchema, fields, headerSchema.size());
    return RowFactory.create(fields);
  }

  static void protoToRow(MessageOrBuilder msg, StructType schema, Object[] values, int offset) {
    List<Descriptors.FieldDescriptor> fields = msg.getDescriptorForType().getFields();
    List<StructField> types = scala.collection.JavaConversions.seqAsJavaList(schema);
    for (int i = 0; i < fields.size(); ++i) {
      Object v = msg.getField(fields.get(i));
      values[offset + i] = unwrap(v, types.get(i).dataType());
    }
  }

  private static Object unwrap(Object v, DataType ty) {
    if (v instanceof MessageOrBuilder) {
      StructType nestedType = (StructType) ty;
      Object[] nestedValue = new Object[nestedType.size()];
      protoToRow((MessageOrBuilder) v, nestedType, nestedValue, 0);
      return RowFactory.create(nestedValue);
    } else if (v instanceof ByteString) {
      return ((ByteString) v).toByteArray();
    } else if (v instanceof List) {
      List<?> l = (List<?>) v;
      Object[] array = new Object[l.size()];
      ArrayType aty = (ArrayType) ty;
      for (int i = 0; i < l.size(); ++i) {
        array[i] = unwrap(l.get(i), aty.elementType());
      }
      return array;
    } else if (v instanceof Descriptors.EnumValueDescriptor) {
      return v.toString();
    } else {
      return v;
    }
  }
}
