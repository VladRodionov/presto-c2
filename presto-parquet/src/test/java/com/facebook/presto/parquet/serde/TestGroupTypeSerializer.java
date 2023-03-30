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

import org.apache.parquet.schema.GroupType;
import org.testng.annotations.Test;

import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

public class TestGroupTypeSerializer extends TestSerializerBase{

  @Test
  public void testGroupTypeSerializer() {
    Output out = new Output(1 << 16);
    GroupType gt = TestUtils.createRandomGroupType(100);
    kryo.writeObject(out, gt);
    Input in = new Input(out.getBuffer());
    GroupType read = kryo.readObject(in, GroupType.class);
    assertTrue(TestUtils.equals(gt, read));
    
    out.reset();
    
    gt = TestUtils.createRandomGroupTypeWithSubGroup(100);
    kryo.writeObject(out, gt);
    in = new Input(out.getBuffer());
    read = kryo.readObject(in, GroupType.class);
    assertTrue(TestUtils.equals(gt, read));
  }
}
