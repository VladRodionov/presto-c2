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

import org.apache.parquet.schema.ColumnOrder;
import org.testng.annotations.Test;

import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

public class TestColumnOrderSerializer extends TestSerializerBase {

  @Test
  public void testColumnOrderSerializer() {
    
    ColumnOrder undefined = ColumnOrder.undefined();
    Output out = new Output(1 << 16);
    kryo.writeObject(out, undefined);
    byte[] buf = out.getBuffer();
    Input in = new Input(buf);
    ColumnOrder order = kryo.readObject(in, ColumnOrder.class);
    assertEquals(undefined, order);
    
    out.reset();
    ColumnOrder defined = ColumnOrder.typeDefined();
    kryo.writeObject(out, defined);
    buf = out.getBuffer();
    in = new Input(buf);
    order = kryo.readObject(in, ColumnOrder.class);
    assertEquals(defined, order);
  }
}
