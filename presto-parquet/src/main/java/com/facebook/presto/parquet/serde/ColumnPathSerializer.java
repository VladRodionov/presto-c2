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

import org.apache.parquet.hadoop.metadata.ColumnPath;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

final public class ColumnPathSerializer extends Serializer<ColumnPath> {

  @Override
  public void write(Kryo kryo, Output output, ColumnPath object) {
    String[] paths = object.toArray();
    int size = paths.length;
    output.writeInt(size);
    for(int i = 0; i < size; i++) {
      output.writeString(paths[i]);
    }
  }

  @Override
  public ColumnPath read(Kryo kryo, Input input, Class<? extends ColumnPath> type) {
    int size = input.readInt();
    String[] paths = new String[size];
    for(int i = 0; i < size; i++) {
      paths[i] = input.readString();
    }
    return ColumnPath.get(paths);
  }
}