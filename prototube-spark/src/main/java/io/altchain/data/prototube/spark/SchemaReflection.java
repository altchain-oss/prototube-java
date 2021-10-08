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

import com.google.protobuf.Descriptors;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType$;
import org.apache.spark.sql.types.BooleanType$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType$;
import org.apache.spark.sql.types.FloatType$;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.EnumMap;
import java.util.HashMap;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.BOOLEAN;
import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.BYTE_STRING;
import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.DOUBLE;
import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.ENUM;
import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.FLOAT;
import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.INT;
import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.LONG;
import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.STRING;

final class SchemaReflection {
  private static final EnumMap<Descriptors.FieldDescriptor.JavaType, DataType> DATATYPE_MAP;

  private SchemaReflection() {
  }

  static {
    HashMap<Descriptors.FieldDescriptor.JavaType, DataType> m = new HashMap<>();
    m.put(BOOLEAN, BooleanType$.MODULE$);
    m.put(INT, IntegerType$.MODULE$);
    m.put(LONG, LongType$.MODULE$);
    m.put(FLOAT, FloatType$.MODULE$);
    m.put(DOUBLE, DoubleType$.MODULE$);
    m.put(STRING, StringType$.MODULE$);
    m.put(BYTE_STRING, BinaryType$.MODULE$);
    m.put(ENUM, StringType$.MODULE$);
    DATATYPE_MAP = new EnumMap<>(m);
  }

  static StructType headerSchema(Descriptors.Descriptor desc) {
    StructField[] fields = desc.getFields().stream().map(x -> structFieldFor(x, "_pbtb_"))
        .toArray(StructField[]::new);
    return new StructType(fields);
  }

  static StructType schemaFor(Descriptors.Descriptor desc) {
    StructField[] fields = desc.getFields().stream().map(x -> structFieldFor(x, ""))
        .toArray(StructField[]::new);
    return new StructType(fields);
  }

  private static StructField structFieldFor(Descriptors.FieldDescriptor fd, String prefix) {
    DataType dataType = DATATYPE_MAP.get(fd.getJavaType());
    if (dataType == null) {
      StructField[] fields = fd.getMessageType().getFields().stream()
          .map(x -> structFieldFor(x, prefix)).toArray(StructField[]::new);
      dataType = new StructType(fields);
    }
    DataType ty = fd.isRepeated() ? new ArrayType(dataType, false) : dataType;
    boolean nullable = !fd.isRequired() && !fd.isRepeated();
    return new StructField(
        prefix + fd.getName(), ty, nullable, Metadata.empty()
    );
  }
}
