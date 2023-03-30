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

import java.util.List;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

final public class BlockMetaDataSerializer extends Serializer<BlockMetaData> {

  @Override
  public void write(Kryo kryo, Output output, BlockMetaData object) {
    List<ColumnChunkMetaData> list = object.getColumns();
    output.writeInt(list.size());
    for (int i = 0; i < list.size(); i++) {
      kryo.writeObject(output, list.get(i));
    }
    long rowCount = object.getRowCount();
    output.writeLong(rowCount);
    long totalByteSize = object.getTotalByteSize();
    output.writeLong(totalByteSize);
    String path = object.getPath();
    output.writeString(path);
    int ordinal = object.getOrdinal();
    output.writeInt(ordinal);
  }

  @Override
  public BlockMetaData read(Kryo kryo, Input input, Class<? extends BlockMetaData> type) {
    BlockMetaData block = new BlockMetaData();
    int size = input.readInt();
    for (int i = 0; i < size; i++) {
      block.addColumn(kryo.readObject(input, ColumnChunkMetaData.class));
    }
    long rowCount = input.readLong();
    long totalByteSize = input.readLong();
    String path = input.readString();
    int ordinal = input.readInt();
    block.setOrdinal(ordinal);
    block.setPath(path);
    block.setTotalByteSize(totalByteSize);
    block.setRowCount(rowCount);
    return block;
  }
}