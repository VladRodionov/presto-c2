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

import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.facebook.presto.parquet.cache.ParquetFileMetadata;

final public class ParquetFileMetadataSerializer extends Serializer<ParquetFileMetadata> {

  @Override
  public void write(Kryo kryo, Output output, ParquetFileMetadata object) {
    int size = object.getMetadataSize();
    long modTime = object.getModificationTime();
    ParquetMetadata metaData = object.getParquetMetadata();
    output.writeInt(size);
    output.writeLong(modTime);
    kryo.writeObject(output, metaData);
  }

  @Override
  public ParquetFileMetadata read(Kryo kryo, Input input, 
      Class<? extends ParquetFileMetadata> type) {
    int size = input.readInt();
    long modTime = input.readLong();
    ParquetMetadata pm = kryo.readObject(input, ParquetMetadata.class);
    return new ParquetFileMetadata(pm, size, modTime);
  }
}
