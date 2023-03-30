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

import static org.testng.Assert.assertTrue;

import java.util.Arrays;

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.testng.annotations.Test;

import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

public class TestStatisticsSerializer extends TestSerializerBase {
  
  @Test
  public void testStaisticsSerializer() {
    PrimitiveTypeName[] types = new PrimitiveTypeName[] {
      PrimitiveTypeName.BOOLEAN,
      PrimitiveTypeName.BINARY,
      PrimitiveTypeName.INT32,
      PrimitiveTypeName.INT64,
      PrimitiveTypeName.INT96,
      PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
      PrimitiveTypeName.FLOAT,
      PrimitiveTypeName.DOUBLE
    };
    
    Arrays.stream(types).forEach(x -> runForType(x));
    // Test null
    Output out = new Output(1<< 16);
    kryo.writeObjectOrNull(out,  null, Statistics.class);
    byte[] buf = out.getBuffer();
    Input in = new Input(buf);
    Statistics<?> read = kryo.readObjectOrNull(in, Statistics.class);
    assertTrue(read == null);
  }
  
  private void runForType(PrimitiveTypeName typeName) {
    PrimitiveType type = TestUtils.createRandomPrimitiveType(typeName);
    Statistics<?> stats = TestUtils.createStatistics(type);
    Output out = new Output(1<< 16);
    kryo.writeObject(out,  stats);
    byte[] buf = out.getBuffer();
    Input in = new Input(buf);
    Statistics<?> read = kryo.readObject(in, Statistics.class);
    assertTrue(stats.equals(read));
  }
}
