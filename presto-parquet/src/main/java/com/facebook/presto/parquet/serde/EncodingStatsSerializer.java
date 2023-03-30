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

import java.util.Iterator;
import java.util.Set;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

final public class EncodingStatsSerializer extends Serializer<EncodingStats> {

  @Override
  public void write(Kryo kryo, Output output, EncodingStats object) {
    Set<Encoding> dict = object.getDictionaryEncodings();
    Set<Encoding> data = object.getDataEncodings();
    boolean useV2 = object.usesV2Pages();
    
    output.writeInt(dict.size());
    Iterator<Encoding> it = dict.iterator();
    while(it.hasNext()) {
      Encoding e = it.next();
      int num = object.getNumDictionaryPagesEncodedAs(e);
      kryo.writeObject(output, e);
      output.writeInt(num);
    }
    output.writeInt(data.size());
    it = data.iterator();
    while(it.hasNext()) {
      Encoding e = it.next();
      int num = object.getNumDataPagesEncodedAs(e);
      kryo.writeObject(output, e);
      output.writeInt(num);
    }
    output.writeBoolean(useV2);
  }

  @Override
  public EncodingStats read(Kryo kryo, Input input, Class<? extends EncodingStats> type) {
    EncodingStats.Builder builder = new EncodingStats.Builder();
    int dictSize = input.readInt();
    for (int i = 0; i < dictSize; i++) {
      Encoding e = kryo.readObject(input,  Encoding.class);
      int n = input.readInt();
      builder.addDictEncoding(e, n);
    }
    
    int dataSize = input.readInt();
    for (int i = 0; i < dataSize; i++) {
      Encoding e = kryo.readObject(input,  Encoding.class);
      int n = input.readInt();
      builder.addDataEncoding(e, n);
    }
    boolean useV2 = input.readBoolean();
    if (useV2) {
      builder.withV2Pages();
    }
    return builder.build();
  }
}
