/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.facebook.presto.parquet.serde;

import static org.testng.Assert.assertTrue;

import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.testng.annotations.Test;

import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

public class TestColumnChunkMetaDataSerializer extends TestSerializerBase {

  @Test
  public void testColumnChunkMetaDataSerializer() {
    Output out = new Output(1 << 20);
    
    for (int i = 0; i < 1000; i++) {
      ColumnChunkMetaData data = TestUtils.createRandomColumnChunkMetaData();
      kryo.writeObject(out, data);
      byte[] buf = out.getBuffer();
      Input in = new Input(buf);
      ColumnChunkMetaData read = kryo.readObject(in, ColumnChunkMetaData.class);
      assertTrue(TestUtils.equals(data, read));
      out.reset();
    }
  }
}
