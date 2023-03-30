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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.ID;
import org.apache.parquet.schema.Type.Repetition;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

final public class GroupTypeSerializer extends Serializer<GroupType> {
  private Constructor<GroupType> cstr;
   
   public GroupTypeSerializer() {
     try {
         cstr = GroupType.class.getDeclaredConstructor(
           Repetition.class,
           String.class,
           LogicalTypeAnnotation.class,
           List.class,
           ID.class);
         cstr.setAccessible(true); 
     } catch (NoSuchMethodException | SecurityException e) {
       // TODO Auto-generated catch block
       e.printStackTrace();
     }
   }
   @Override
   public void write(Kryo kryo, Output output, GroupType object) {
     Repetition rep = object.getRepetition();
     String name = object.getName();
     LogicalTypeAnnotation lta = object.getLogicalTypeAnnotation();
     List<Type> fields = object.getFields();
     ID id = object.getId();
     kryo.writeObject(output,  rep);
     output.writeString(name);
     kryo.writeObjectOrNull(output, lta, LogicalTypeAnnotation.class);
     int size = fields.size();
     output.writeInt(size);
     for (int i = 0; i < size; i++) {
       Type t = fields.get(i);
       TypeOfType tot = TypeOfType.get(t);
       output.writeInt(tot.ordinal());
       kryo.writeObject(output, t);
     }
     kryo.writeObjectOrNull(output,  id, ID.class);
   }
   
   @Override
   public GroupType read(Kryo kryo, Input input, Class<? extends GroupType> type) {
     Repetition rep = kryo.readObject(input,  Repetition.class);
     String name = input.readString();
     LogicalTypeAnnotation lta = kryo.readObjectOrNull(input, LogicalTypeAnnotation.class);
     int size = input.readInt();
     ArrayList<Type> list = new ArrayList<>();
     for (int i = 0; i < size; i++) {
       int ordinal = input.readInt();
       TypeOfType tot = TypeOfType.values()[ordinal];
       Type t = null;
       switch(tot) {
         case GROUP_TYPE:
           t = kryo.readObject(input, GroupType.class);
           break;
         case MESSAGE_TYPE:
           t = kryo.readObject(input, MessageType.class);
           break;
         case PRIMITIVE_TYPE:  
           t = kryo.readObject(input, PrimitiveType.class);
           break;
       }
       list.add(t);
     }
     
     ID id = kryo.readObjectOrNull(input,  ID.class);
     try {
       return cstr.newInstance(rep, name, lta, list, id);
     } catch(Exception e) {
       e.printStackTrace();
     }
     return null;
   }
 }
 