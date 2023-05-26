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

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;

public class HiveBloomFilterSerializer extends Serializer<HiveBloomFilter>{

  @Override
  public void write(Kryo kryo, Output output, HiveBloomFilter object) {
    // can be null
    if (object == null) {
      output.writeByte(0);
      return;
    } else {
      output.writeByte(1);
    }
    long[] bits = object.getBitSet();
    int bitSize = object.getBitSize();
    int numFunctions = object.getNumHashFunctions();
    output.writeInt(bits.length);
    //TODO: what is the maximum size of the filter?
    output.writeLongs(bits, 0, bits.length);
    output.writeInt(bitSize);
    output.writeInt(numFunctions);
  }

  @Override
  public HiveBloomFilter read(Kryo kryo, Input input, Class<? extends HiveBloomFilter> type) {
    int isNull = input.readByte();
    if (isNull == 0) {
      return null;
    }
    int size = input.readInt();
    long[] data = input.readLongs(size);
    int bitSize = input.readInt();
    int numFunctions = input.readInt();
    
    return new HiveBloomFilter(data, bitSize, numFunctions);
  }

}
