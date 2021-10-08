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

import io.altchain.data.prototube.idl.Test.TestStruct;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

public class RecordDeserializerTest {
  @Test
  public void testDeserializeProtoToRow() {
    TestStruct testTemp = TestStruct.newBuilder()
            .setIntValue(5)
            .setEnumValue(TestStruct.TestEnum.ENUM_2)
            .build();

    final StructType schema =  SchemaReflection.schemaFor(testTemp.getDescriptorForType());
    Object[] values = new Object[schema.size()];
    RecordDeserializer.protoToRow(testTemp, schema, values, 0);
    Assert.assertEquals(5L, values[0]);
    Assert.assertEquals("ENUM_2", values[1]);
  }
}
