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

import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.testng.annotations.Test;

import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.facebook.presto.parquet.cache.ParquetFileMetadata;

public class TestParquetFileMetadataSerializer extends TestSerializerBase {

  @Test
  public void testParquetFileMetadataSerializer() {
    Output out = new Output(1 << 22, -1);
    for (int i = 0; i < 10; i++) {
      ParquetMetadata meta = TestUtils.createRandomParquetMetadata();
      int size = 1000000;
      long modTime = System.currentTimeMillis();
      ParquetFileMetadata fm = new ParquetFileMetadata(meta, size, modTime);
      kryo.writeObject(out, fm);
      byte[] buf = out.getBuffer();
      Input in = new Input(buf);
      ParquetFileMetadata  read = kryo.readObject(in, ParquetFileMetadata.class);
      assertTrue(TestUtils.equals(fm.getParquetMetadata(), read.getParquetMetadata()));
      assertTrue(fm.getMetadataSize() == read.getMetadataSize());
      assertTrue(fm.getModificationTime() == read.getModificationTime());
      out.reset();
    }
  }
}
