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
import java.lang.reflect.InvocationTargetException;

import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.ID;
import org.apache.parquet.schema.Type.Repetition;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

final public class PrimitiveTypeSerializer extends Serializer<PrimitiveType> {

  private Constructor<PrimitiveType> cstr;
  
  public PrimitiveTypeSerializer() {
    try {
        cstr = PrimitiveType.class.getDeclaredConstructor(
        Repetition.class,
        PrimitiveTypeName.class,
        int.class,
        String.class,
        LogicalTypeAnnotation.class,
        ID.class,
        ColumnOrder.class);
        cstr.setAccessible(true); 
    } catch (NoSuchMethodException | SecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  @Override
  public void write(Kryo kryo, Output output, PrimitiveType object) {
    Repetition repetition = object.getRepetition();
    PrimitiveTypeName primitive = object.getPrimitiveTypeName();
    int length = object.getTypeLength();
    String name = object.getName(); 
    LogicalTypeAnnotation logicalTypeAnnotation = object.getLogicalTypeAnnotation();
    ID id = object.getId();
    ColumnOrder columnOrder = object.columnOrder();  
    
    kryo.writeObjectOrNull(output, repetition, Repetition.class);
    kryo.writeObjectOrNull(output, primitive, PrimitiveTypeName.class);
    output.writeInt(length);
    output.writeString(name);
    kryo.writeObjectOrNull(output, logicalTypeAnnotation, LogicalTypeAnnotation.class);
    kryo.writeObjectOrNull(output, id, ID.class);
    kryo.writeObjectOrNull(output, columnOrder, ColumnOrder.class);
    
  }

  @Override
  public PrimitiveType read(Kryo kryo, Input input, Class<? extends PrimitiveType> type) {
    Repetition repetition = kryo.readObjectOrNull(input, Repetition.class);
    PrimitiveTypeName primitive = kryo.readObjectOrNull(input, PrimitiveTypeName.class);
    int length = input.readInt();
    String name = input.readString(); 
    LogicalTypeAnnotation logicalTypeAnnotation = kryo.readObjectOrNull(input, LogicalTypeAnnotation.class);
    ID id = kryo.readObjectOrNull(input, ID.class);
    ColumnOrder columnOrder = kryo.readObjectOrNull(input, ColumnOrder.class); 
    //TODO
    try {
      return cstr.newInstance(repetition, primitive, length, name, 
        logicalTypeAnnotation, id, columnOrder);
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    }
    return null;
  }
  
  public Constructor<PrimitiveType> getConstructor(){
    return cstr;
  }
}