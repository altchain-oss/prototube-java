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

import io.altchain.data.prototube.MessageSerializer;
import io.altchain.data.prototube.idl.Test.TestStruct;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class RowDataDeserializationSchemaTest {
  private static final byte[] PT_MESSAGE;

  static {
    TestStruct msg = TestStruct.newBuilder()
        .setIntValue(1).setEnumValue(TestStruct.TestEnum.ENUM_1)
        .addSpam(TestStruct.NestedMessageLevel2.newBuilder()
            .setBar(TestStruct.NestedMessage.newBuilder().addFoo(2))
        ).build();
    PT_MESSAGE = MessageSerializer.serialize(msg, 0);
  }

  @Test
  public void testNestedSchema() throws Exception {
    Map.Entry<RowType, DataTypes.Field[]> recordSchema = SchemaReflection.schemaFor(TestStruct.getDescriptor());
    RowType recordType = recordSchema.getKey();
    DeserializationSchema<RowData> schema = RowDataDeserializationSchema.create(
        TestStruct.class.getName(), null, recordType);
    DeserializationSchema.InitializationContext ctx = mock(DeserializationSchema.InitializationContext.class);
    schema.open(ctx);

    RowData data = schema.deserialize(PT_MESSAGE);
    assertEquals(1, data.getLong(0));
    assertEquals("ENUM_1", data.getString(1).toString());
    Object[] arr = ((GenericArrayData) data.getArray(2)).toObjectArray();
    assertEquals(1, arr.length);
    RowData spam = (RowData) arr[0];
    RowData bar = spam.getRow(0, 1);
    Object[] arr2 = ((GenericArrayData) bar.getArray(0)).toObjectArray();
    assertEquals(1, arr2.length);
    assertEquals(2L, arr2[0]);
  }

  @Test
  public void testSkippedFields() throws Exception {
    Map.Entry<RowType, DataTypes.Field[]> recordSchema = SchemaReflection.schemaFor(TestStruct.getDescriptor());
    RowType recordTypeOld = recordSchema.getKey();
    List<RowType.RowField> fields = new ArrayList<>(recordTypeOld.getFields());
    fields.remove(1);
    RowType recordType = new RowType(fields);

    DeserializationSchema<RowData> schema = RowDataDeserializationSchema.create(
        TestStruct.class.getName(), null, recordType);
    DeserializationSchema.InitializationContext ctx = mock(DeserializationSchema.InitializationContext.class);
    schema.open(ctx);

    RowData data = schema.deserialize(PT_MESSAGE);
    assertEquals(1, data.getLong(0));
    assertEquals(2, data.getArity());
    Object[] arr = ((GenericArrayData) data.getArray(1)).toObjectArray();
    assertEquals(1, arr.length);
    RowData spam = (RowData) arr[0];
    RowData bar = spam.getRow(0, 1);
    Object[] arr2 = ((GenericArrayData) bar.getArray(0)).toObjectArray();
    assertEquals(1, arr2.length);
    assertEquals(2L, arr2[0]);
  }
}
