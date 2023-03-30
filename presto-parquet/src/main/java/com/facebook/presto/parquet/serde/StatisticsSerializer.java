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

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

@SuppressWarnings("rawtypes")
final public class StatisticsSerializer extends Serializer<Statistics> {

  @Override
  public void write(Kryo kryo, Output output, Statistics object) {
    // Statistics can be null in ColumnChunkMetaData
    if (object == null) {
      output.writeByte(0);
    } else {
      output.writeByte(1);
    }
    PrimitiveType type = object.type();
    byte[] min = object.getMinBytes();
    byte[] max = object.getMaxBytes();
    long numNulls = object.getNumNulls();
    kryo.writeObject(output, type);
    int minSize = min == null? 0: min.length;
    int maxSize = max == null? 0: max.length;
    output.writeInt(minSize);
    if (min != null) {
      output.write(min);
    }
    output.writeInt(maxSize);
    if (max != null) {
      output.write(max);
    }
    output.writeLong(numNulls);
  }

  @Override
  public Statistics read(Kryo kryo, Input input, Class<? extends Statistics> type) {
    int v = input.readByte();
    if (v == 0) {
      return null;
    } 
    PrimitiveType t = kryo.readObject(input, PrimitiveType.class);
    Statistics.Builder builder = Statistics.getBuilderForReading(t);
    int minSize = input.readInt();
    byte[] min = minSize == 0? null: input.readBytes(minSize);
    int maxSize = input.readInt();
    byte[] max = maxSize == 0? null: input.readBytes(maxSize);
    long numNulls = input.readLong();      
    return builder.withMax(max).withMin(min).withNumNulls(numNulls).build();
  }
}