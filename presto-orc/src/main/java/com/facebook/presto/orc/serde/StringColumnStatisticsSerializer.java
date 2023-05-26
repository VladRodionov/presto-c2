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
import com.facebook.presto.orc.metadata.statistics.StringColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.StringStatistics;

import io.airlift.slice.Slice;

public class StringColumnStatisticsSerializer extends Serializer<StringColumnStatistics>{

  @Override
  public void write(Kryo kryo, Output output, StringColumnStatistics object) {
    output.writeByte((byte)StatisticsKind.STRING.ordinal());

    long numValues = object.getNumberOfValues();
    HiveBloomFilter filter = object.getBloomFilter();
    StringStatistics ss = object.getStringStatistics();
    long sum = ss.getSum();
    // Can be null
    Slice min = ss.getMin();
    // Can be null
    Slice max = ss.getMax();
    output.writeLong(numValues);
    kryo.writeObjectOrNull(output, filter, HiveBloomFilter.class);
    
    output.writeLong(sum);
    if (min == null) {
      output.writeByte(0);
    } else {
      output.writeByte(1);
      kryo.writeObject(output,  min);
    }
    if (max == null) {
      output.writeByte(0);
    } else {
      output.writeByte(1);
      kryo.writeObject(output,  max);
    }
  }

  @Override
  public StringColumnStatistics read(Kryo kryo, Input input,
      Class<? extends StringColumnStatistics> type) {
    long numValues = input.readLong();
    HiveBloomFilter filter = kryo.readObjectOrNull(input, HiveBloomFilter.class);
    long sum = input.readLong();
    Slice min = null, max = null;
    boolean notNull = input.readByte() != 0;
    if (notNull) {
      min = kryo.readObject(input, Slice.class);
    }
    notNull = input.readByte() != 0;
    if (notNull) {
      max = kryo.readObject(input, Slice.class);
    }
    StringStatistics ss = new StringStatistics(min, max, sum);
    return new StringColumnStatistics(numValues, filter, ss);
  }

}
