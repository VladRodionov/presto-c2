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

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

final public class MessageTypeSerializer extends Serializer<MessageType> {

  @Override
  public void write(Kryo kryo, Output output, MessageType object) {
    String name = object.getName();
    output.writeString(name);
    List<Type> fields = object.getFields();
    int size = fields.size();
    output.writeInt(size);
    for(int i = 0; i < size; i++) {
      TypeOfType tot = TypeOfType.get(fields.get(i));
      output.writeInt(tot.ordinal());
      kryo.writeObject(output, fields.get(i));
    }
  }

  @Override
  public MessageType read(Kryo kryo, Input input, Class<? extends MessageType> type) {
    String name = input.readString();
    int size = input.readInt();
    ArrayList<Type> fields = new ArrayList<Type>(size);
    for (int i = 0; i < size; i++) {
      int ordinal = input.readInt();
      TypeOfType tot = TypeOfType.values()[ordinal];
      switch(tot) {
        case MESSAGE_TYPE:  
          fields.add(kryo.readObject(input, MessageType.class));
          break;
        case GROUP_TYPE: 
          fields.add(kryo.readObject(input, GroupType.class));
          break;
        case PRIMITIVE_TYPE:
          fields.add(kryo.readObject(input, PrimitiveType.class));
      }
    }
    return new MessageType(name, fields);
  }

}
