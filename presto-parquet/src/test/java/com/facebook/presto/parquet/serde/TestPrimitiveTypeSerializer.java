/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.parquet.serde;

import static org.testng.Assert.assertEquals;

import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.Test;

import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;


public class TestPrimitiveTypeSerializer extends TestSerializerBase {

  @Test
  public void testPrimitiveTypeSerializer() {    
    PrimitiveType primitiveType = null;  
    Output out = new Output(1 << 16);
    for (int i =0; i < 100; i++) {
      primitiveType = TestUtils.createRandomPrimitiveType();
      kryo.writeObject(out, primitiveType);
      byte[] buf = out.getBuffer();
      Input in = new Input(buf);
      PrimitiveType type = kryo.readObject(in, PrimitiveType.class);
      assertEquals(primitiveType.hashCode(), type.hashCode());
      out.reset();
    }
  }
}
