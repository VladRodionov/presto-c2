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

import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

final public class ParquetMetadataSerializer extends Serializer<ParquetMetadata> {

  @Override
  public void write(Kryo kryo, Output output, ParquetMetadata object) {
    FileMetaData fm  = object.getFileMetaData();
    List<BlockMetaData> blocks = object.getBlocks();
    kryo.writeObject(output, fm);
    output.writeInt(blocks.size());
    for(int i = 0; i < blocks.size(); i++) {
      kryo.writeObject(output, blocks.get(i));
    }
  }

  @Override
  public ParquetMetadata read(Kryo kryo, Input input, Class<? extends ParquetMetadata> type) {
    FileMetaData fm = kryo.readObject(input, FileMetaData.class);
    int size = input.readInt();
    ArrayList<BlockMetaData> blocks = new ArrayList<BlockMetaData>(size);
    for (int i = 0; i < size; i++) {
      blocks.add(kryo.readObject(input,  BlockMetaData.class));
    }
    return new ParquetMetadata(fm, blocks);
  }
}
