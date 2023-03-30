package com.facebook.presto.parquet.serde;

import java.util.HashMap;
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
import java.util.Map;

import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.schema.MessageType;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

final public class FileMetaDataSerializer extends Serializer<FileMetaData> {

  @Override
  public void write(Kryo kryo, Output output, FileMetaData object) {
     MessageType mt = object.getSchema();
     String createdBy = object.getCreatedBy();
     Map<String, String> map = object.getKeyValueMetaData();
     
     kryo.writeObject(output,  mt);
     output.writeString(createdBy);
     int size = map != null? map.size(): 0;
     output.writeInt(size);
     for (Map.Entry<String, String> entry: map.entrySet()) {
       output.writeString(entry.getKey());
       output.writeString(entry.getValue());
     }    
  }

  @Override
  public FileMetaData read(Kryo kryo, Input input, Class<? extends FileMetaData> type) {
    MessageType mt = kryo.readObject(input, MessageType.class);
    String createdBy = input.readString();
    int size = input.readInt();
    HashMap<String, String> map = new HashMap<String, String>();
    for (int i = 0; i < size; i++) {
      String key = input.readString();
      String value = input.readString();
      map.put(key, value);    
    }
    return new FileMetaData(mt, map, createdBy);
  }
}
