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
package com.facebook.presto.orc.serde;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;

import org.testng.annotations.Test;

import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;

public class TestHiveBloomFilterSerializer extends TestSerializerBase {

  @Test
  public void testHiveBloomFilterSerializer() {
    HiveBloomFilter filter = TestUtils.getBloomFilter();

    long[] bits = filter.getBitSet();
    int numBits = filter.getBitSize();
    int numHashes = filter.getNumHashFunctions();
    
    Output out = new Output(1 << 16);
    kryo.writeObject(out, filter);
    Input in = new Input(out.getBuffer());
    HiveBloomFilter read = kryo.readObject(in, HiveBloomFilter.class);
    
    long[] $bits = read.getBitSet();
    int $numBits = read.getBitSize();
    int $numHashes = read.getNumHashFunctions();
    assertEquals(numBits, $numBits);
    assertEquals(numHashes, $numHashes);
    assertTrue(Arrays.equals(bits, $bits));
    
    // Test null
    out.reset();
    kryo.writeObjectOrNull(out, null, HiveBloomFilter.class);
    in = new Input(out.getBuffer());
    read = kryo.readObject(in, HiveBloomFilter.class);
    assertNull(read);
    
  }
}
