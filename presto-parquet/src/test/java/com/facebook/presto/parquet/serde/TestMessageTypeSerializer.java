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

import static org.testng.Assert.assertTrue;

import java.util.List;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.testng.annotations.Test;

import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

public class TestMessageTypeSerializer extends TestSerializerBase{

  @Test
  public void testMessageTypeSerializer() {
    Output out = new Output(1 << 20);
    String name = "name";
    List<Type> list = TestUtils.randomListOfTypes(100);
    MessageType mt = new MessageType(name, list);
    
    kryo.writeObject(out, mt);
    Input in = new Input(out.getBuffer());
    MessageType read = kryo.readObject(in, MessageType.class);
    assertTrue(TestUtils.equals(mt, read));
    
    out.reset();
    
    GroupType gt = TestUtils.createRandomGroupTypeWithSubGroup(100);
    list.add(gt);
    mt = new MessageType(name, list);

    kryo.writeObject(out, mt);
    in = new Input(out.getBuffer());
    read = kryo.readObject(in, MessageType.class);
    assertTrue(TestUtils.equals(mt, read));
  }
}
