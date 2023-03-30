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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

final public class ColumnChunkMetaDataSerializer extends Serializer<ColumnChunkMetaData> {

  @SuppressWarnings("rawtypes")
  @Override
  public void write(Kryo kryo, Output output, ColumnChunkMetaData object) {
    ColumnPath path = object.getPath();
    PrimitiveType type = object.getPrimitiveType();
    CompressionCodecName codec = object.getCodec();
    EncodingStats encodingStats = object.getEncodingStats();
    Set<Encoding> encodings = object.getEncodings();
    Statistics statistics = object.getStatistics();
    long firstDataPage = object.getFirstDataPageOffset();
    long dictionaryPageOffset = object.getDictionaryPageOffset();
    long valueCount = object.getValueCount();
    long totalSize = object.getTotalSize();
    long totalUncompressedSize = object.getTotalUncompressedSize();
    
    kryo.writeObject(output, path);
    kryo.writeObject(output, type);
    kryo.writeObject(output, codec);
    kryo.writeObject(output, encodingStats);
    int setSize = encodings.size();
    output.writeInt(setSize);
    Iterator<Encoding> it = encodings.iterator();
    while(it.hasNext()) {
      kryo.writeObject(output,  it.next());
    }
    kryo.writeObject(output, statistics);
    output.writeLong(firstDataPage);
    output.writeLong(dictionaryPageOffset);
    output.writeLong(valueCount);
    output.writeLong(totalSize);
    output.writeLong(totalUncompressedSize);
  }
  
  @SuppressWarnings("rawtypes")
  @Override
  public ColumnChunkMetaData read(Kryo kryo, Input input, Class<? extends ColumnChunkMetaData> type) {
    ColumnPath path = kryo.readObject(input, ColumnPath.class);
    PrimitiveType ptype = kryo.readObject(input, PrimitiveType.class);
    CompressionCodecName codec = kryo.readObject(input, CompressionCodecName.class);
    EncodingStats encodingStats = kryo.readObject(input, EncodingStats.class);
    Set<Encoding> encodings = new HashSet<Encoding>();
    int setSize = input.readInt();
    for (int i = 0; i < setSize; i++) {
      encodings.add(kryo.readObject(input, Encoding.class));
    }
    Statistics statistics = kryo.readObject(input, Statistics.class);
    // There are two sub-classes of interest: IntColumnChunkMetaData and LongColumnChunkMetaData
    // they treat differently below numbers. For Long class we write them as is, for Int class
    // we need translate them before writing
    //  value = value + Integer.MIN_VALUE
    long firstDataPage = input.readLong();
    long dictionaryPageOffset = input.readLong();
    long valueCount = input.readLong();
    long totalSize = input.readLong();
    long totalUncompressedSize = input.readLong();
    return ColumnChunkMetaData.get(path, ptype, codec, encodingStats, encodings, statistics, 
      firstDataPage, dictionaryPageOffset, valueCount, totalSize, totalUncompressedSize);
  }
}