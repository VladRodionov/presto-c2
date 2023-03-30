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

import org.apache.parquet.schema.ColumnOrder;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

final public class ColumnOrderSerializer extends Serializer<ColumnOrder> {

  @Override
  public void write(Kryo kryo, Output output, ColumnOrder object) {
    ColumnOrder.ColumnOrderName name = object.getColumnOrderName();
    if (name == ColumnOrder.ColumnOrderName.UNDEFINED) {
      output.writeBoolean(false);
    } else {
      output.writeBoolean(true);
    }
  }

  @Override
  public ColumnOrder read(Kryo kryo, Input input, Class<? extends ColumnOrder> type) {
    boolean name = input.readBoolean();
    if (name) {
      return ColumnOrder.typeDefined();
    } else {
      return ColumnOrder.undefined();
    }
  }
}
