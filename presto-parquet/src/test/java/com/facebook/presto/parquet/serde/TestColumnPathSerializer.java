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

import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.testng.annotations.Test;

import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

public class TestColumnPathSerializer extends TestSerializerBase {

  @Test
  public void testColumnPathSerializer() {
    ColumnPath path = TestUtils.getColumnPath(15);
    Output out = new Output(1000);
    kryo.writeObject(out, path);
    Input in = new Input(out.getBuffer());
    ColumnPath read = kryo.readObject(in,  ColumnPath.class);
    assertEquals(path, read);
  }
}
